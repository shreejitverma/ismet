# Behaviour Notes

Rules callers depend on that are easy to miss in the API reference.

## Orders and risk

- Every ismet timeout now keeps a caller's cancel on Python 3.10 and 3.11: `asyncio.wait_for` there hands a cancelled caller the result when the awaited operation finishes in the same loop turn (CPython gh-86296), so a cancelled task could carry on.
  `ismet._aio.wait_for` applies the 3.12 rule on every version.
- `RiskConfig.limit_currency` declares the currency of notional limits, and an order in another currency is refused with check `limit_currency`.
- Without it, an order's notional limits read in its own instrument's currency.
- A gross limit fails closed when holdings are unknown, a mark or multiplier is missing, or exposure spans currencies.
- A gross limit never uses a broker's average price. Marks come from reference quotes no older than `max_reference_age_seconds`, or from the latest fill, so a quote outage refuses new risk while orders that only reduce a position still pass.
- A zero price is no price for a notional limit, whether it comes from the order or the quote.
- An instrument with a listing or delisting date needs a calendar, because the date is judged at the venue, not in UTC.
- `restricted_symbols` entries match regardless of case, so `AAPL@xnas` and `aapl` both match, and `BRKb` also restricts a ticker spelled `BRKB`.
- `cancel_all` sends cancels concurrently, at most 32 at a time, so one slow answer does not hold back the rest of a kill sweep.
- A cancel during the kill-switch read after the send has been journalled closes the order as `CANCELED_BEFORE_SEND`, because the venue was never called, and then reraises `CancelledError`.
- An amendment re-reads the kill switch from the journal after `ReplaceRequested` is written, so an engage from another process refuses it with `KILL_SWITCH`, an unreadable journal with `JOURNAL_UNREADABLE`, and a cancel during the read with `CANCELED_BEFORE_SEND`.
  An amendment overtaken by a cancel or a last fill is not sent, and terms it left on an order that is still open are cleared with `ReplaceRejected` code `CANCELED_BEFORE_SEND`.
- A disputed fee keeps the first report's amount and currency together; only the venue order id and liquidity are merged from the second.
- `ismet._aio.wait_for` reports `TimeoutError` for a cancelled operation only when that wait itself cancelled it.
- An operation that ends cancelled on its own raises `CancelledError`, on 3.10 and 3.11 as well as 3.12.

## Journal

- Record kinds gained `position_adjusted` and `sweep_mark`.
- `POSITION_BASELINE` no longer records a price, since cost basis is not a mark.
- `export_jsonl` gained `raw_payload`, the exact text the hash covers, and `unique_intent`, and `verify_jsonl` checks an archive with those.
- An anchor ahead of the record being appended is kept with no critical log when the anchored record is still present with the same digest, since another process wrote it.
- The anchor is updated after the record commits, under the database write lock, so writers in different processes cannot move it backward and a crash leaves it behind, never ahead.
- An anchor update whose commit fails is rolled back, so the next append still runs.

## Accounting

- A ledger identifies a fill by `(client_order_id, fill_id)`, since venues promise execution ids unique only per order, and the same identity with different terms raises.
- `build_ledger` sorts by execution time alone, so same-time fills keep their input order. Pass them in execution order.
- `Valuation.fills_with_unknown_fee` above zero means `fees` is a lower bound and `net` an upper bound.

## Adapter conformance

- `run_trading_conformance` requires `DuplicateOrderId` for a repeated client order id, and fails a stream that ends before reporting.
- `unknown_status_error=` declares a documented exception to "status of an unknown order is `None`". `FixProvider` uses it with `TransportError`, because its only record is local and a wiped store must not make a live order look dead.
- `SimVenue.answer_ticks`, `after_answer`, and `retain_busted_fills` let a test interleave stream events with venue answers.
- An elected `SimVenue` stop, including one that has already partially filled, takes liquidity when a later quote appears, even if that quote has traded back through the stop.
- `run_conformance` adds `instrument_symbol`, failing an instrument answered for another symbol, and fails bars spaced by anything other than a whole number of the asked interval.
  For intervals longer than an hour, a gap longer than one interval that is a whole number plus or minus exactly one hour also passes, since a daylight-saving change moves a local-time grid by an hour and falls in a closed period.
- `MockProvider.bars` serves only bars that have closed by its clock, so a range reaching past now stops at the last closed bar.
- `run_conformance` asks for bars up to the close of the bar still forming, so its `lookback` now ends one interval later than the last whole minute.
  Its `bars_closed` check fails a bar that closes more than `MAX_PLAUSIBLE_CLOCK_SKEW` after the answer arrived.
  An intraday bar closes one interval after it opens, and a daily bar closes with its session in the venue's calendar; weekly and monthly bars are not checked.
- `Provider.bars_every_interval` declares that `bars` returns a bar for every closed interval in which the venue's calendar is open, intervals without trades included.
  It defaults to `False`, and `MockProvider` declares it.
  For a declaring provider, the `bars_complete` check fails any such interval missing between the first bar returned and the start of the range, between two bars, or after the last bar up to the request.
  Intervals are counted on the grid of the bars returned, and one that closed within `MAX_PLAUSIBLE_CLOCK_SKEW` before the request may be absent.
  Without the declaration, a venue without a calendar, or a range outside the calendar's data, the check is skipped and says why.
- The release-workflow test also flags a gate step that pipes without pipefail, or runs an `&&` list on any line but the last, since GitHub's default `bash -e` hides either failure.

## FIX

- `FixProvider.release_quarantine(*, operator, reason, expected)` is now `async` and durable.
- A new option is `tls`, and a new session setting is `session_period`.
- Removed: `FixTags`, `default_venue`, `sync_on_logon`, and the `FixDialect` overrides `status_request_ids`, `unknown_id_business_reasons`, `duplicate_order_reasons`, and `ignored_msg_types`.
- `FixStore` gained `open_orders`, `fills_since`, `busted_executions`, `reset_period`, `set_reset_period`, `release_quarantine`, and `put_order(..., is_open=)`.
- A provider declaring `venues` refuses an order on any other venue.
- Tag 44 is read only for `LIMIT` and `STOP_LIMIT`, so an echoed zero cannot price a stop order at nothing.
- A PossResend report `FixProvider` flags as lost does not lower the stored filled quantity below the fills booked and does not move a terminal order back to a working status.
  Any other report flagged as lost, a PossDup resend included since it replays in sequence, carries the venue's current CumQty, which the stored filled quantity follows, so later reports on that order match again.

## Market data

- `Provider.gates_streams` says an adapter's streams already drop repeats and flag gaps. On such a provider the client's `gap_policy` and `on_gap` follow those flags, `on_gap` reports `missing=0`, and the first quote after a reconnect counts as a gap, so it raises under `GapPolicy.RAISE`.
- Providers take `ws_on_state`, and `WebSocketTransport(is_data=)` decides what counts as progress, so a server that only acknowledges and closes trips `max_reconnects`.
- `QualityThresholds.confirmation_band_bps` (100 by default) sets how close consecutive prices must be to confirm a jump as real.
- A closed bar's age counts from its close, and the check is skipped for monthly bars.
- `BookBuilder.apply_snapshot` returns `False` for a snapshot older than a dropped delta, and the book stays out of sync.
- A Coinbase `/book` response with no `time` raises rather than looking fresh.
- A Binance 418 ban or a 429 with `Retry-After` is remembered, and requests are refused locally until the wait ends.
  A ban lasts at least 2 minutes, whatever the response or the host clock says.
- A 429 or 418 answering a Binance stream handshake sets the same hold for requests and for other streams.
  Whether Binance answers handshakes this way has not been checked.
- A Binance hold of over 20 seconds raises `RateLimited` from a new stream or from one with `ws_max_reconnects`, and any other stream logs a WARNING with the remaining time and waits.

## Calendars

- `TradingCalendar.extended_through(last, closed=..., source=...)` returns a copy covering dates up to `last`, continuing the last era's hours, closed on the listed weekdays, and recording `source`.
- `ismet doctor` warns 90 days before a calendar's data ends and names both remedies, upgrading or extending from the venue's holiday list.

## Observability

- `ismet.otel.OpenTelemetryMetrics`, from the `otel` extra, implements `Metrics` on an OpenTelemetry meter, and refuses a negative counter increment.

## Reference data

- `SecurityMaster.universe(venue, on, known_at=)` resolves each symbol by the same rule as `resolve`.
- A tie on `known_from` between different securities raises `ValidationError`.
- For a `CorporateAction`, the latest known wins per `(kind, ex_date)`. Two cash dividends on one ex-date both apply only if they share a `known_from`.
- `adjust_bars` needs the bar of the session before a dividend's ex-date, or an explicit `price_factor`. With `known_at` it also skips actions whose ex-date has not arrived at the venue. It uses `calendar=` or the venue's bundled calendar.

## Storage and export

- `BarStore.read` and `checksum` with no `as_of` apply no knowledge cutoff.
- `backfill` raises when `start >= end`, or when `window` is shorter than the interval's longest period, since such a window could hold no bar and never finish.
- `backfill` defaults to a 30 day window, 31 for monthly bars, and a tail shorter than one period joins the window before it.
- A progress write that fails, or a stored row that no longer parses, counts as that window's failure, and the remaining windows still run.
- A stored row needs its venue and currency well formed, not registered in the reading process, so `BarStore.read` and `ismet journal orders` work in a process that never called `register_private_venue` or `register_crypto_asset`.
- `BackfillReport` gained `windows_incomplete` and `sessions_missing`.
- Reformatting a price is not a revision, so existing stored bars with trailing zeros write one spurious revision after upgrading.
- `to_records` and dataframes gained `timeliness`, and carry `quality` and `source`.
- `backfill` leaves a window unfinished when it ends within one period of the store clock, or when a fetched bar is `STALE`, so a rerun fetches it again.

## Sync client

- A full sync-stream buffer waits for room without holding a default-executor worker, so one slow consumer cannot stall every other stream.

## CLI

- `--start` and `--end` refuse naive times.
- `kill` and `journal` refuse abbreviated flags.
- `ismet providers` lists names, with capabilities only from `ismet providers <name>`.
- Non-table lists print as TOON `- ` items.
- `ismet kill engage` that cannot append the halt says the halt was not recorded and to cancel at the venue directly.
- A no-op `ismet kill engage` includes the `chain` field.
- `ismet kill engage` checks the chain only after the halt is recorded, so a long verification never delays it.
