#!/usr/bin/env Rscript
# ==============================================================================
# +EV ALERT — CLOUD / ALWAYS-ON BUILD  (Source 2 only, fully self-contained)
#
# Needs ONLY: R + data.table + httr2 + a DATAGOLF_API_KEY env var.
# No targets store, no model, no pipeline files — runs on any tiny always-on box.
#
# Each run: scans cross-book matchup/3-ball lines, flags book prices that beat
# DataGolf's fair line by >= mu_ev_min, pushes NEW ones to your phone (ntfy),
# paper-trades them, and auto-grades closing-line value (CLV) when an event's
# lines lock (R1 start, via the in-play feed). Paper-trading until CLV proves out.
#
# Schedule it every 20 min (cron / GitHub Actions). State persists in ./golf_picks.
# ==============================================================================

suppressPackageStartupMessages({ library(data.table); library(httr2) })
msg <- function(...) cat(format(Sys.time(), "[%H:%M:%S]"), ..., "\n", sep = " ")

CONFIG <- list(
  ntfy_server      = "https://ntfy.sh",
  ntfy_topic       = "golf-ev-a7f3k9q2x8",   # subscribe to this in the ntfy app
  push_enabled     = TRUE,
  mu_ev_min        = 0.02,
  books_exclude    = c("bovada","unibet","betway","pointsbet","skybet","williamhill",
                       "pinnacle","betcris","betonline"),
  log_file         = "golf_picks/alerts_log.csv",
  ledger_file      = "golf_picks/clv_ledger.rds",
  clv_results_file = "golf_picks/clv_results.csv",
  max_push_lines   = 12
)

API_KEY <- Sys.getenv("DATAGOLF_API_KEY")
if (nchar(API_KEY) == 0) stop("Set DATAGOLF_API_KEY env var")
if (!dir.exists("golf_picks")) dir.create("golf_picks", recursive = TRUE)

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0 || all(is.na(a))) b else a
inv <- function(x) ifelse(is.na(x) | x <= 1, NA_real_, 1 / x)

# decimal -> american (display only; CLV math stays in decimal)
dec_to_american <- function(d) {
  d <- as.numeric(d)
  ifelse(is.na(d), NA_real_, ifelse(d >= 2, round((d - 1) * 100), round(-100 / (d - 1))))
}
am_str <- function(d) ifelse(is.na(d), "NA", sprintf("%+d", as.integer(dec_to_american(d))))

dg_get <- function(endpoint, params = list()) {
  params$key <- API_KEY; params$file_format <- "json"
  resp <- tryCatch(request(paste0("https://feeds.datagolf.com/", endpoint)) |>
                     req_url_query(!!!params) |> req_perform(),
                   error = function(e) NULL)
  if (is.null(resp) || resp_status(resp) != 200) return(NULL)
  resp_body_json(resp, simplifyVector = TRUE)
}

push_phone <- function(title, body, tags = "money_with_wings", priority = "default") {
  if (!CONFIG$push_enabled) { msg("  [push off] ", title); return(invisible()) }
  ok <- tryCatch({
    request(paste0(CONFIG$ntfy_server, "/", CONFIG$ntfy_topic)) |>
      req_headers(Title = title, Priority = priority, Tags = tags) |>
      req_body_raw(charToRaw(body)) |> req_perform(); TRUE
  }, error = function(e) { msg("  push failed: ", conditionMessage(e)); FALSE })
  if (ok) msg("  pushed: ", title)
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
        market = market, book = bk, selection = pnm[[s]][keep], opponent = opp_nm[keep],
        sel_id = as.integer(pid[[s]][keep]), opp_id = opp_ids[keep],
        odds = as.character(round(bo[keep], 2)), fair_p = round(fp[keep], 4),
        ev = round(bo[keep] * fp[keep] - 1, 4),
        bet_key = paste(market, bk, as.character(pid[[s]][keep]), opp_ids[keep]))
    }
  }
  if (length(rows) == 0) return(NULL)
  out <- rbindlist(rows, use.names = TRUE, fill = TRUE)
  out[, event := if (!is.null(m$event_name)) m$event_name else NA_character_]
  out[]
}

scan_all <- function() {
  all <- rbindlist(lapply(c("tournament_matchups","round_matchups","3_balls"),
                          function(mk){ r <- scan_market(mk); Sys.sleep(0.4); r }),
                   use.names = TRUE, fill = TRUE)
  if (is.null(all) || nrow(all) == 0) return(NULL)
  all[, odds_num := suppressWarnings(as.numeric(odds))]
  all[]
}

get_inplay <- function() {
  j <- dg_get("preds/in-play", list(tour = "pga"))
  if (is.null(j) || is.null(j$info)) return(NULL)
  list(event = j$info$event_name %||% NA_character_,
       round = suppressWarnings(as.integer(j$info$current_round %||% NA)))
}

# CONGESTION (validated early-market momentum signal): a player's starts in the
# prior 35 days. Opening matchup lines underweight it. Computed from the recent
# PGA schedule + fields; cached once/day. Degrades to NULL if feeds are down.
get_congestion <- function() {
  cache <- "golf_picks/congestion_cache.rds"
  if (file.exists(cache)) { c <- tryCatch(readRDS(cache), error = function(e) NULL)
    if (!is.null(c) && identical(c$date, Sys.Date())) return(c$tab) }
  yr <- as.integer(format(Sys.Date(), "%Y"))
  el <- tryCatch(as.data.table(dg_get("historical-raw-data/event-list", list(tour = "pga", year = yr))),
                 error = function(e) NULL)
  if (is.null(el) || !"date" %in% names(el)) return(NULL)
  if (!"event_id" %in% names(el) && "id" %in% names(el)) setnames(el, "id", "event_id")
  el[, d := as.Date(date)]
  recent <- unique(el[!is.na(d) & d >= Sys.Date() - 35 & d < Sys.Date(), .(event_id = as.character(event_id))])
  if (!nrow(recent)) { tab <- data.table(dg_id = integer(), cong = integer())
    saveRDS(list(date = Sys.Date(), tab = tab), cache); return(tab) }
  ids <- integer(0)
  for (eid in recent$event_id) {
    rr <- tryCatch(as.data.table(dg_get("historical-raw-data/rounds",
                    list(tour = "pga", event_id = eid, year = yr))), error = function(e) NULL)
    Sys.sleep(0.3)
    if (is.null(rr)) next
    idc <- grep("dg_id", names(rr), value = TRUE)[1]
    if (!is.na(idc)) ids <- c(ids, as.integer(rr[[idc]]))
  }
  tab <- as.data.table(table(dg_id = ids))
  if (nrow(tab)) { setnames(tab, c("dg_id", "cong"))
    tab[, `:=`(dg_id = as.integer(as.character(dg_id)), cong = as.integer(cong))] }
  saveRDS(list(date = Sys.Date(), tab = tab), cache)
  tab
}

# attach sel/opp congestion and the momentum edge (sel minus opponent(s))
attach_congestion <- function(board) {
  cong <- tryCatch(get_congestion(), error = function(e) NULL)
  if (is.null(cong) || !nrow(cong)) { board[, cong_edge := NA_real_]; return(board) }
  setkey(cong, dg_id)
  board[, sel_cong := cong[.(sel_id), cong]]
  board[, opp_cong := vapply(strsplit(as.character(opp_id), ","), function(z) {
    v <- cong[.(as.integer(z)), cong]; if (all(is.na(v))) NA_real_ else mean(v, na.rm = TRUE) }, numeric(1))]
  board[is.na(sel_cong), sel_cong := 0]; board[is.na(opp_cong), opp_cong := 0]
  board[, cong_edge := sel_cong - opp_cong]
  board[]
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

main <- function() {
  msg("=== +EV ALERT (cloud, CLV paper-trade) ===")
  ledger <- if (file.exists(CONFIG$ledger_file)) as.data.table(readRDS(CONFIG$ledger_file)) else EMPTY_LEDGER()
  board <- scan_all()
  cur_event <- NA_character_; now <- as.character(Sys.time())

  if (!is.null(board) && nrow(board) > 0) {
    board <- attach_congestion(board)          # add momentum co-signal
    cur_event <- board$event[!is.na(board$event)][1] %||% NA_character_
    msg("Event: ", cur_event, " | scanned ", nrow(board), " lines | +EV>=", CONFIG$mu_ev_min,
        ": ", sum(board$ev >= CONFIG$mu_ev_min),
        " | congestion-backed +EV: ", sum(board$ev >= CONFIG$mu_ev_min &
          !is.na(board$cong_edge) & board$cong_edge >= 1, na.rm = TRUE))

    open_keys <- ledger[graded == FALSE, bet_key]
    upd <- board[bet_key %in% open_keys]
    if (nrow(upd) > 0) for (i in seq_len(nrow(upd)))
      ledger[bet_key == upd$bet_key[i] & graded == FALSE,
             `:=`(close_odds = upd$odds_num[i], close_dg_fair = upd$fair_p[i], close_ts = now)]

    flagged <- board[ev >= CONFIG$mu_ev_min]
    new_flag <- flagged[!bet_key %in% ledger$bet_key]
    if (nrow(new_flag) > 0) {
      # congestion-backed bets first (co-signal), then by EV
      new_flag[, cong_backed := !is.na(cong_edge) & cong_edge >= 1]
      setorder(new_flag, -cong_backed, -ev)
      add <- new_flag[, .(event, market, book, selection, opponent, bet_key,
                          bet_odds = odds_num, bet_dg_fair = fair_p, bet_ev = ev, bet_ts = now,
                          bet_cong_edge = round(cong_edge, 1), cong_backed,
                          close_odds = odds_num, close_dg_fair = fair_p, close_ts = now,
                          pushed = TRUE, graded = FALSE)]
      ledger <- rbindlist(list(ledger, add), use.names = TRUE, fill = TRUE)
      lines <- add[, sprintf("%s%+5.1f%% EV  %s %s vs %s @ %s [%s]",
                  ifelse(cong_backed, "★ ", "  "),
                  bet_ev*100, book, substr(selection,1,20), substr(opponent,1,16),
                  am_str(bet_odds), market)]
      body <- paste(head(lines, CONFIG$max_push_lines), collapse = "\n")
      if (nrow(add) > CONFIG$max_push_lines) body <- paste0(body, "\n…+", nrow(add)-CONFIG$max_push_lines, " more")
      ncb <- sum(add$cong_backed, na.rm = TRUE)
      push_phone(sprintf("%d new +EV golf bets (best %+.1f%%%s)", nrow(add), max(add$bet_ev)*100,
                         if (ncb > 0) sprintf(", %d ★momentum", ncb) else ""),
                 body, priority = if (max(add$bet_ev) >= 0.05 || ncb > 0) "high" else "default")
      fwrite(copy(add)[, `:=`(ts = now, odds_american = am_str(bet_odds))],
             CONFIG$log_file, append = file.exists(CONFIG$log_file))
      msg(nrow(add), " new paper bets placed + pushed")
    } else msg("No new +EV bets (", nrow(flagged), " flagged, all tracked).")
  } else msg("No matchup lines available right now.")

  ip <- get_inplay()
  for (ev_nm in unique(ledger[graded == FALSE, event])) {
    started <- !is.null(ip) && !is.na(ip$round) && ip$round >= 1 && event_match(ev_nm, ip$event)
    rolled  <- !is.na(cur_event) && !event_match(ev_nm, cur_event)
    if (!(started || rolled)) next
    g <- ledger[event == ev_nm & graded == FALSE]; if (nrow(g) == 0) next
    g[, clv := bet_odds / close_odds - 1]; g[, beat := clv > 0]
    g[, ev_close := bet_odds * close_dg_fair - 1]
    rep <- sprintf("%s — %d paper bets | mean CLV %+.2f%% | beat-close %.0f%% | EV@close %+.2f%%",
                   ev_nm, nrow(g), mean(g$clv)*100, mean(g$beat)*100, mean(g$ev_close)*100)
    msg("GRADE: ", rep)
    push_phone(paste0("CLV report: ", substr(ev_nm, 1, 28)),
               paste0(rep, "\n", if (mean(g$clv) > 0 && mean(g$beat) > 0.5)
                        "OK beat the close — edge looks real"
                      else "X did not beat the close — likely stale lines, NOT proven edge"),
               tags = "bar_chart")
    fwrite(copy(g)[, `:=`(graded_ts = now,
                          bet_odds_american = am_str(bet_odds),
                          close_odds_american = am_str(close_odds))],
           CONFIG$clv_results_file, append = file.exists(CONFIG$clv_results_file))
    ledger[event == ev_nm & graded == FALSE, graded := TRUE]
  }

  saveRDS(ledger, CONFIG$ledger_file)
  msg("Ledger: ", nrow(ledger), " bets (", sum(!ledger$graded), " open, ", sum(ledger$graded), " graded).")
}

if (!interactive()) main()
