# Behaviour Notes

Rules callers depend on that are easy to miss in the API reference.

## Orders and risk

- `RiskConfig.limit_currency` declares the currency of notional limits, and an order in another currency is refused with check `limit_currency`.
- Without it, an order's notional limits read in its own instrument's currency.
- A gross limit fails closed when holdings are unknown, a mark or multiplier is missing, or exposure spans currencies.
- A gross limit never uses a broker's average price. Marks come from reference quotes no older than `max_reference_age_seconds`, or from the latest fill, so a quote outage refuses new risk while orders that only reduce a position still pass.
- A zero price is no price for a notional limit, whether it comes from the order or the quote.
- An instrument with a listing or delisting date needs a calendar, because the date is judged at the venue, not in UTC.
- `restricted_symbols` entries match regardless of case, so `AAPL@xnas` and `aapl` both match, and `BRKb` also restricts a ticker spelled `BRKB`.
- `cancel_all` sends cancels concurrently, at most 32 at a time, so one slow answer does not hold back the rest of a kill sweep.

## Journal

- Record kinds gained `position_adjusted` and `sweep_mark`.
- `POSITION_BASELINE` no longer records a price, since cost basis is not a mark.
- `export_jsonl` gained `raw_payload`, the exact text the hash covers, and `unique_intent`, and `verify_jsonl` checks an archive with those.

## Accounting

- A ledger identifies a fill by `(client_order_id, fill_id)`, since venues promise execution ids unique only per order, and the same identity with different terms raises.
- `build_ledger` sorts by execution time alone, so same-time fills keep their input order. Pass them in execution order.
- `Valuation.fills_with_unknown_fee` above zero means `fees` is a lower bound and `net` an upper bound.

## Adapter conformance

- `run_trading_conformance` requires `DuplicateOrderId` for a repeated client order id, and fails a stream that ends before reporting.
- `unknown_status_error=` declares a documented exception to "status of an unknown order is `None`". `FixProvider` uses it with `TransportError`, because its only record is local and a wiped store must not make a live order look dead.
- `SimVenue.answer_ticks`, `after_answer`, and `retain_busted_fills` let a test interleave stream events with venue answers.

## FIX

- `FixProvider.release_quarantine(*, operator, reason)` is now `async` and durable.
- New options are `tls` and `allow_plaintext_credentials`, and a new session setting is `session_period`.
- Removed: `FixTags`, `default_venue`, `sync_on_logon`, and the `FixDialect` overrides `status_request_ids`, `unknown_id_business_reasons`, `duplicate_order_reasons`, and `ignored_msg_types`.
- `FixStore` gained `open_orders`, `fills_since`, `busted_executions`, `prune_messages_sent_before`, `reset_period`, `set_reset_period`, `release_quarantine`, and `put_order(..., is_open=)`.
- A provider declaring `venues` refuses an order on any other venue.
- Tag 44 is read only for `LIMIT` and `STOP_LIMIT`, so an echoed zero cannot price a stop order at nothing.

## Market data

- `Provider.gates_streams` says an adapter's streams already drop repeats and flag gaps. On such a provider the client's `gap_policy` and `on_gap` follow those flags, `on_gap` reports `missing=0`, and the first quote after a reconnect counts as a gap, so it raises under `GapPolicy.RAISE`.
- Providers take `ws_on_state`, and `WebSocketTransport(is_data=)` decides what counts as progress, so a server that only acknowledges and closes trips `max_reconnects`.
- `QualityThresholds.confirmation_band_bps` (100 by default) sets how close consecutive prices must be to confirm a jump as real.
- A closed bar's age counts from its close, and the check is skipped for monthly bars.
- `BookBuilder.apply_snapshot` returns `False` for a snapshot older than a dropped delta, and the book stays out of sync.
- A Coinbase `/book` response with no `time` raises rather than looking fresh.
- A Binance 418 ban is remembered, and requests are refused locally until it ends.

## Reference data

- `SecurityMaster.universe(venue, on, known_at=)` resolves each symbol by the same rule as `resolve`.
- A tie on `known_from` between different securities raises `ValidationError`.
- For a `CorporateAction`, the latest known wins per `(kind, ex_date)`. Two cash dividends on one ex-date both apply only if they share a `known_from`.
- `adjust_bars` needs the bar of the session before a dividend's ex-date, or an explicit `price_factor`. With `known_at` it also skips actions whose ex-date has not arrived at the venue. It uses `calendar=` or the venue's bundled calendar.

## Storage and export

- `BarStore.read` and `checksum` with no `as_of` apply no knowledge cutoff.
- `backfill` raises when `start >= end`.
- `BackfillReport` gained `windows_incomplete` and `sessions_missing`.
- Reformatting a price is not a revision, so existing stored bars with trailing zeros write one spurious revision after upgrading.
- `to_records` and dataframes gained `timeliness`, and carry `quality` and `source`.

## CLI

- `--start` and `--end` refuse naive times.
- `kill` and `journal` refuse abbreviated flags.
- `ismet providers` lists names, with capabilities only from `ismet providers <name>`.
- Non-table lists print as TOON `- ` items.
