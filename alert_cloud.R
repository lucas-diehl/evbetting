#!/usr/bin/env Rscript
# ==============================================================================
# +EV BET ALERT SYSTEM  (standalone, scheduled)
#
# Watches TWO +EV sources for the current PGA event and pushes NEW qualifying
# bets to your phone:
#   SOURCE 1  Top-20 model edges   — reuses the targets pipeline (top20_wf model
#             + feature table) to score the live field vs DraftKings top-20 odds.
#   SOURCE 2  Cross-book matchup/3-ball CLV — book prices that beat DataGolf's
#             fair line (market-vs-prior edge).
#
# Delivery: push to phone via ntfy.sh (install the "ntfy" app, subscribe to the
#           private topic below). Dedupes via a state file so you're only pinged
#           on NEW or improved bets. Also writes a dated alert CSV.
#
# Run on a schedule (Windows Task Scheduler) every ~30 min during tournament
# weeks:  Rscript claude\alert_system.R
# ==============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(httr2)
})

msg <- function(...) cat(format(Sys.time(), "[%H:%M:%S]"), ..., "\n", sep = " ")

# ------------------------------------------------------------------------------
CONFIG <- list(
  # --- Push (ntfy.sh): install ntfy app, subscribe to this exact topic ---
  ntfy_server  = "https://ntfy.sh",
  ntfy_topic   = "golf-ev-a7f3k9q2x8",   # << private topic; change if you like
  push_enabled = TRUE,

  # --- SOURCE 1: top-20 model — DISABLED (market-equivalent out-of-sample) ---
  t20_enabled    = FALSE,
  t20_ev_min     = 0.015, t20_edge_min = 0.02, t20_conf_min = 0.12,
  t20_odds_min   = -500,  t20_odds_max = 600,  t20_max_per_ev = 2, t20_alpha = 0.70,

  # --- SOURCE 2: matchup/3-ball CLV (the deployed edge) ---
  mu_ev_min      = 0.02,    # book price beats DG fair by >= 2%
  books_exclude  = c("bovada", "unibet", "betway", "pointsbet", "skybet", "williamhill",
                     "pinnacle", "betcris", "betonline"),

  # --- paper-trade ledger + CLV grading ---
  log_file          = "golf_picks/alerts_log.csv",     # bets alerted
  ledger_file       = "golf_picks/clv_ledger.rds",     # paper bets + rolling close
  clv_results_file  = "golf_picks/clv_results.csv",    # per-bet CLV after grading
  max_push_lines    = 12
)

API_KEY <- Sys.getenv("DATAGOLF_API_KEY")
if (nchar(API_KEY) == 0) stop("Set DATAGOLF_API_KEY env var")
if (!dir.exists("golf_picks")) dir.create("golf_picks", recursive = TRUE)

# ------------------------------------------------------------------------------
# PUSH (ntfy)
# ------------------------------------------------------------------------------
push_phone <- function(title, body, tags = "money_with_wings", priority = "default") {
  if (!CONFIG$push_enabled) { msg("  [push disabled] ", title); return(invisible()) }
  url <- paste0(CONFIG$ntfy_server, "/", CONFIG$ntfy_topic)
  ok <- tryCatch({
    request(url) |>
      req_headers(Title = title, Priority = priority, Tags = tags) |>
      req_body_raw(charToRaw(body)) |>
      req_perform()
    TRUE
  }, error = function(e) { msg("  push failed: ", conditionMessage(e)); FALSE })
  if (ok) msg("  pushed: ", title)
}

inv <- function(x) ifelse(is.na(x) | x <= 1, NA_real_, 1 / x)

# decimal -> american (display only; CLV math stays in decimal)
dec_to_american <- function(d) {
  d <- as.numeric(d)
  ifelse(is.na(d), NA_real_, ifelse(d >= 2, round((d - 1) * 100), round(-100 / (d - 1))))
}
am_str <- function(d) ifelse(is.na(d), "NA", sprintf("%+d", as.integer(dec_to_american(d))))

# ------------------------------------------------------------------------------
# SOURCE 1: live top-20 model edges (reuse targets store; no rebuild)
# ------------------------------------------------------------------------------
source_top20 <- function() {
  out <- tryCatch({
    suppressPackageStartupMessages({
      library(targets); library(tidymodels); library(workflows)
    })
    if (!dir.exists("_targets/objects")) { msg("  [t20] no targets store — skip"); return(NULL) }

    # live-path functions
    source("dg_pull/dg_api.R");                 source("dg_pull/dg_pull_betting_tools.R")
    source("R/live_helpers.R");                 source("R/asof_join.R")
    source("R/dk_edges_top20.R");               source("R/sim_top20.R")

    # Numeric-safe override: the pipeline's american_to_prob() returns a LOGICAL
    # vector for all-NA input (live odds have no close yet), which breaks fifelse
    # in join_dk_open_close. Wrapping in as.numeric() keeps it double.
    american_to_prob <<- function(odds) {
      odds <- as.numeric(odds)
      as.numeric(ifelse(odds > 0, 100 / (odds + 100), (-odds) / ((-odds) + 100)))
    }

    model <- targets::tar_read(top20_wf)
    feat  <- as.data.table(targets::tar_read(rounds_feat_long_all_plus_2025))

    odds <- dg_betting_outrights(tour = "pga", market = "top_20", odds_format = "american")
    odds <- as.data.table(odds)
    if (nrow(odds) == 0) { msg("  [t20] no live DK top-20 odds posted yet"); return(NULL) }
    eid  <- live_event_id_from_odds(odds, tour_tag = "PGA")
    odds <- stamp_live_event_id(odds, eid)

    o <- as.data.table(odds)
    if (!("dg_id" %in% names(o))) { if ("player_id" %in% names(o)) o[, dg_id := player_id]
                                    else { msg("  [t20] odds missing dg_id"); return(NULL) } }
    ep <- unique(o[, .(event_id, player_id = as.integer(dg_id))])
    ep[, start_date := Sys.Date()]

    feats <- as.data.table(join_asof_features(feat, ep))
    if (!nrow(feats)) { msg("  [t20] no features matched live field"); return(NULL) }

    # Field-relative normalization — replicate model_table_all so the model gets
    # the columns it was trained on (the pipeline's live path omits these).
    feats[, field_size := .N]
    feats[, is_alt := 0L]
    feats[, major_flag := as.integer(grepl("masters|championship|open|players",
                                           tolower(eid)))]
    feats[, field_mean_sg24  := mean(sg24,  na.rm = TRUE)]
    feats[, field_sd_sg24    := sd(sg24,    na.rm = TRUE)]
    feats[, field_mean_sg100 := mean(sg100, na.rm = TRUE)]
    feats[, field_sd_sg100   := sd(sg100,   na.rm = TRUE)]
    feats[, z_sg24  := (sg24  - field_mean_sg24)  / field_sd_sg24]
    feats[, z_sg100 := (sg100 - field_mean_sg100) / field_sd_sg100]
    feats[, pct_sg24  := frank(sg24,  ties.method = "average") / .N]
    feats[, pct_sg100 := frank(sg100, ties.method = "average") / .N]
    feats[, delta_sg24  := sg24  - field_mean_sg24]
    feats[, delta_sg100 := sg100 - field_mean_sg100]
    feats[, event_base_rate := 20 / field_size]
    if (!("dummy" %in% names(feats))) feats[, dummy := 1]

    prob  <- predict(model, new_data = feats, type = "prob")
    p_raw <- if (".pred_1" %in% names(prob)) prob[[".pred_1"]] else prob[[ncol(prob)]]
    feats[, p_top20 := as.numeric(p_raw)]
    pred <- feats[, .(event_id, player_id, p_top20)]

    j <- join_dk_open_close(pred, odds, alpha = CONFIG$t20_alpha,
                            odds_min = CONFIG$t20_odds_min, odds_max = CONFIG$t20_odds_max)
    if (!nrow(j)) { msg("  [t20] DK join empty"); return(NULL) }

    bets <- select_bets_top20(j, ev_col = "ev_blend_per_1", ev_min = CONFIG$t20_ev_min,
                              max_bets_per_event = CONFIG$t20_max_per_ev,
                              odds_min = CONFIG$t20_odds_min, odds_max = CONFIG$t20_odds_max)
    if (!nrow(bets)) { msg("  [t20] 0 bets pass thresholds"); return(NULL) }

    # apply remaining README gates: edge + confidence
    edge_col <- if ("edge_open_devig" %in% names(bets)) "edge_open_devig" else "edge_open"
    bets <- bets[get(edge_col) >= CONFIG$t20_edge_min & p_top20 >= CONFIG$t20_conf_min]
    if (!nrow(bets)) { msg("  [t20] 0 bets after edge/conf gates"); return(NULL) }

    nm <- if ("player_name" %in% names(bets)) bets$player_name else as.character(bets$player_id)
    res <- data.table(
      source = "TOP20", market = "top_20_DK", book = "DraftKings",
      selection = paste0(nm, " Top-20"), opponent = "field",
      odds = sprintf("%+d", as.integer(bets$open_odds)),
      model_p = round(bets$p_top20, 4),
      fair_p  = round(bets$p_open_devig, 4),
      edge    = round(bets[[edge_col]], 4),
      ev      = round(bets$ev_blend_per_1, 4),
      bet_key = paste("top20", bets$event_id, bets$player_id))
    msg("  [t20] ", nrow(res), " qualifying bets")
    res
  }, error = function(e) { msg("  [t20] error: ", conditionMessage(e)); NULL })
  out
}

# ------------------------------------------------------------------------------
# SOURCE 2: cross-book matchup / 3-ball CLV vs DG fair line
# ------------------------------------------------------------------------------
dg_get <- function(endpoint, params = list()) {
  params$key <- API_KEY; params$file_format <- "json"
  resp <- tryCatch(request(paste0("https://feeds.datagolf.com/", endpoint)) |>
                     req_url_query(!!!params) |> req_perform(),
                   error = function(e) NULL)
  if (is.null(resp) || resp_status(resp) != 200) return(NULL)
  resp_body_json(resp, simplifyVector = TRUE)
}

scan_market <- function(market) {
  m <- dg_get("betting-tools/matchups",
              list(tour = "pga", market = market, odds_format = "decimal"))
  if (is.null(m) || is.null(m$match_list)) return(NULL)
  ml <- m$match_list; n <- nrow(ml)
  if (is.null(n) || n == 0 || is.null(ml$odds$datagolf)) return(NULL)
  sides <- intersect(c("p1","p2","p3"), names(ml$odds$datagolf))
  dg <- ml$odds$datagolf; has_tie <- "tie" %in% names(dg)
  denom <- Reduce(`+`, lapply(sides, function(s) ifelse(is.na(inv(dg[[s]])), 0, inv(dg[[s]]))))
  if (has_tie) denom <- denom + ifelse(is.na(inv(dg$tie)), 0, inv(dg$tie))
  fair <- lapply(sides, function(s) inv(dg[[s]]) / denom); names(fair) <- sides
  pid <- lapply(sides, function(s) ml[[paste0(s,"_dg_id")]]); names(pid) <- sides
  pnm <- lapply(sides, function(s) ml[[paste0(s,"_player_name")]]); names(pnm) <- sides
  books <- setdiff(names(ml$odds), c("datagolf", CONFIG$books_exclude))
  rows <- list()
  for (bk in books) {
    b <- ml$odds[[bk]]; if (is.null(b)) next
    for (s in sides) {
      if (is.null(b[[s]])) next
      bo <- suppressWarnings(as.numeric(b[[s]])); fp <- fair[[s]]
      keep <- !is.na(bo) & bo > 1 & !is.na(fp)
      if (!any(keep)) next
      others <- setdiff(sides, s)
      opp_ids <- apply(do.call(cbind, lapply(others, function(o) as.character(pid[[o]]))), 1,
                       function(z) paste(sort(z), collapse = ","))
      opp_nm  <- apply(do.call(cbind, lapply(others, function(o) pnm[[o]])), 1,
                       function(z) paste(z, collapse = " / "))
      rows[[paste(bk, s)]] <- data.table(
        source = "CLV", market = market, book = bk,
        selection = pnm[[s]][keep], opponent = opp_nm[keep],
        odds = as.character(round(bo[keep], 2)),
        model_p = NA_real_, fair_p = round(fp[keep], 4),
        ev = round(bo[keep] * fp[keep] - 1, 4),
        bet_key = paste(market, bk, as.character(pid[[s]][keep]), opp_ids[keep]))
    }
  }
  if (length(rows) == 0) return(NULL)
  out <- rbindlist(rows, use.names = TRUE, fill = TRUE)
  out[, event := if (!is.null(m$event_name)) m$event_name else NA_character_]
  out[]
}

# Return ALL allowed-book lines (not just flagged) so the ledger can track each
# bet's true closing price even after it stops being +EV.
scan_all_matchups <- function() {
  all <- rbindlist(lapply(c("tournament_matchups","round_matchups","3_balls"),
                          function(mk){ r <- scan_market(mk); Sys.sleep(0.4); r }),
                   use.names = TRUE, fill = TRUE)
  if (is.null(all) || nrow(all) == 0) { msg("  [clv] no matchup lines"); return(NULL) }
  all[, odds_num := suppressWarnings(as.numeric(odds))]
  msg("  [clv] scanned ", nrow(all), " allowed-book lines | +EV>=",
      CONFIG$mu_ev_min, ": ", sum(all$ev >= CONFIG$mu_ev_min))
  all[]
}

# ------------------------------------------------------------------------------
# Helpers for grading
# ------------------------------------------------------------------------------
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0 || all(is.na(a))) b else a

get_inplay <- function() {
  j <- dg_get("preds/in-play", list(tour = "pga"))
  if (is.null(j) || is.null(j$info)) return(NULL)
  list(event = j$info$event_name %||% NA_character_,
       round = suppressWarnings(as.integer(j$info$current_round %||% NA)))
}

event_match <- function(a, b) {
  if (is.na(a) || is.na(b)) return(FALSE)
  filler <- c("the","a","an","of","at","by","for","in","presented","tournament",
              "open","championship","classic","invitational","challenge","cup","workday")
  wa <- setdiff(strsplit(gsub("[^a-z ]","", tolower(a)), "\\s+")[[1]], filler)
  wb <- setdiff(strsplit(gsub("[^a-z ]","", tolower(b)), "\\s+")[[1]], filler)
  length(intersect(wa, wb)) >= 1
}

EMPTY_LEDGER <- function() data.table(
  event = character(), market = character(), book = character(),
  selection = character(), opponent = character(), bet_key = character(),
  bet_odds = numeric(), bet_dg_fair = numeric(), bet_ev = numeric(), bet_ts = character(),
  close_odds = numeric(), close_dg_fair = numeric(), close_ts = character(),
  pushed = logical(), graded = logical())

# ------------------------------------------------------------------------------
# MAIN — Source 2 only: alert new +EV bets, paper-trade them, grade CLV at close
# ------------------------------------------------------------------------------
main <- function() {
  msg("=== +EV ALERT SYSTEM (Source 2 / CLV, paper-traded) ===")
  ledger <- if (file.exists(CONFIG$ledger_file))
              as.data.table(readRDS(CONFIG$ledger_file)) else EMPTY_LEDGER()

  if (isTRUE(CONFIG$t20_enabled)) { msg("Source 1 (top-20) ..."); s1 <- source_top20() } # off by default

  msg("Source 2: matchup/3-ball CLV (allowed books only) ...")
  board <- scan_all_matchups()
  cur_event <- NA_character_
  now <- as.character(Sys.time())

  if (!is.null(board) && nrow(board) > 0) {
    cur_event <- board$event[!is.na(board$event)][1] %||% NA_character_

    # (1) Update rolling CLOSE for every tracked-open bet still on the board
    open_keys <- ledger[graded == FALSE, bet_key]
    upd <- board[bet_key %in% open_keys]
    if (nrow(upd) > 0) for (i in seq_len(nrow(upd))) {
      ledger[bet_key == upd$bet_key[i] & graded == FALSE,
             `:=`(close_odds = upd$odds_num[i], close_dg_fair = upd$fair_p[i], close_ts = now)]
    }

    # (2) Place paper bets on NEW flagged +EV lines, and push them
    flagged <- board[ev >= CONFIG$mu_ev_min]
    new_flag <- flagged[!bet_key %in% ledger$bet_key]
    if (nrow(new_flag) > 0) {
      setorder(new_flag, -ev)
      add <- new_flag[, .(event, market, book, selection, opponent, bet_key,
                          bet_odds = odds_num, bet_dg_fair = fair_p, bet_ev = ev, bet_ts = now,
                          close_odds = odds_num, close_dg_fair = fair_p, close_ts = now,
                          pushed = TRUE, graded = FALSE)]
      ledger <- rbindlist(list(ledger, add), use.names = TRUE, fill = TRUE)

      lines <- add[, sprintf("%+5.1f%% EV  %s %s vs %s @ %s [%s]",
                  bet_ev*100, book, substr(selection,1,20), substr(opponent,1,16),
                  am_str(bet_odds), market)]
      body <- paste(head(lines, CONFIG$max_push_lines), collapse = "\n")
      if (nrow(add) > CONFIG$max_push_lines) body <- paste0(body, "\n…+", nrow(add)-CONFIG$max_push_lines, " more")
      push_phone(sprintf("%d new +EV golf bets (best %+.1f%%)", nrow(add), max(add$bet_ev)*100),
                 body, priority = if (max(add$bet_ev) >= 0.05) "high" else "default")
      fwrite(copy(add)[, `:=`(ts = now, odds_american = am_str(bet_odds))],
             CONFIG$log_file, append = file.exists(CONFIG$log_file))
      msg(nrow(add), " new paper bets placed + pushed:")
      for (i in seq_len(min(length(lines), 15))) msg("  ", lines[i])
    } else msg("No new +EV bets this run (", nrow(flagged), " currently flagged, all tracked).")
  } else {
    msg("No matchup lines available right now.")
  }

  # (3) GRADE events that have closed (R1 started, or a new event's lines are up)
  ip <- get_inplay()
  open_events <- unique(ledger[graded == FALSE, event])
  for (ev_nm in open_events) {
    started <- !is.null(ip) && !is.na(ip$round) && ip$round >= 1 && event_match(ev_nm, ip$event)
    rolled  <- !is.na(cur_event) && !event_match(ev_nm, cur_event)
    if (!(started || rolled)) next

    g <- ledger[event == ev_nm & graded == FALSE]
    if (nrow(g) == 0) next
    g[, clv := bet_odds / close_odds - 1]              # >0 = beat the close
    g[, beat := clv > 0]
    g[, ev_close := bet_odds * close_dg_fair - 1]      # EV vs DG's closing fair line
    mean_clv <- mean(g$clv); beat_rate <- mean(g$beat)
    rep <- sprintf("%s — %d paper bets | mean CLV %+.2f%% | beat-close %.0f%% | EV@close %+.2f%%",
                   ev_nm, nrow(g), mean_clv*100, beat_rate*100, mean(g$ev_close)*100)
    msg("GRADE: ", rep)
    push_phone(paste0("CLV report: ", substr(ev_nm, 1, 28)),
               paste0(rep, "\n",
                      if (mean_clv > 0 && beat_rate > 0.5) "OK beat the close — edge looks real"
                      else "X did not beat the close — likely stale lines, NOT proven edge"),
               tags = "bar_chart", priority = "default")
    fwrite(copy(g)[, `:=`(graded_ts = now,
                          bet_odds_american = am_str(bet_odds),
                          close_odds_american = am_str(close_odds))],
           CONFIG$clv_results_file, append = file.exists(CONFIG$clv_results_file))
    ledger[event == ev_nm & graded == FALSE, graded := TRUE]
  }

  saveRDS(ledger, CONFIG$ledger_file)
  msg("Ledger: ", nrow(ledger), " paper bets (",
      sum(!ledger$graded), " open, ", sum(ledger$graded), " graded).")
}

if (!interactive()) main()
