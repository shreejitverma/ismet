# Operations Runbook

Commands assume `JOURNAL` is the path of the order journal.

## Emergency: stop trading now

```bash
ismet kill engage --journal "$JOURNAL" --reason "<why>" --operator "<you>"
ismet kill status --journal "$JOURNAL"
```

Every order manager using this journal refuses new orders on its next attempt.
Each one also cancels open orders within `kill_poll_interval` (0.5 seconds by default) and keeps retrying until nothing is open.
Cancels go out concurrently, at most 32 at a time, so a stalled gateway delays the sweep by one timeout per batch rather than per order.
Add `--keep-orders` to stop new orders but leave working orders alone.

A process that is not running cannot cancel anything.
If the order manager is down, cancel at the venue directly, then reconcile on restart.

## Resume after a halt

1. Find out why it stopped: `ismet kill status --journal "$JOURNAL"`.
2. Explain every reconciliation break before releasing.
3. If a position differs for an outside reason, such as a split or a transfer, record it: `ismet position adjust <TICKER@MIC> --journal "$JOURNAL" --delta <signed quantity> --reason "<why>" --operator "<you>"`.
   A running order manager adopts it on its next reconciliation, after verifying the hash chain, and ignores one with no operator.
   In code, `OrderManager.adjust_position` does the same.
4. Release under your own name: `ismet kill release --journal "$JOURNAL" --reason "<why>" --operator "<you>"`.
   A blank `--reason` or `--operator` is a usage error, and every reader ignores a journalled release that names no operator.

A release does not clear a cause that is still there: an unexplained position difference halts again within one reconciliation.

## Restart after a crash

Start the order manager on the same journal.
It takes these steps by itself, and you do not need to edit anything:

1. It verifies the hash chain, and refuses to start if it is broken.
2. It replays the journal, and refuses to start if its risk limits differ from the latest journalled `update_risk_config`.
   Start with the journalled limits, then change them through `update_risk_config`.
   Limits never changed through it come from the constructor.
3. It closes orders that never reached the send step as `NEVER_SENT`.
4. It asks the venue about anything that may have been sent, by client order id.
5. It recovers fills made since the last clean fill sweep on record, less `fill_lookback`.
   That point is a `sweep_mark` journal record, written at most every quarter of `fill_lookback`, so a long query outage or an unrelated record cannot move it forward.
6. It compares positions, and halts on a difference.

To inspect a journal while the process is stopped:

```bash
ismet journal verify "$JOURNAL"
ismet journal orders "$JOURNAL"
```

Never resubmit an order by hand under a new client order id until the old one has been resolved.
A strategy that restarts should resubmit with the same deterministic id, which returns the existing order without sending again.

## Failure modes

| Symptom | Meaning | Action |
|---|---|---|
| Order status `unknown` | The send may have reached the venue, and the response did not return | Wait. Reconcile resolves it. New orders in that symbol are blocked meanwhile. |
| `unknown` that will not clear | The venue's status query is failing | Check connectivity and credentials. A `query_failed` break is reported each pass. |
| Break `orphan_order` | The venue has an open order ismet did not send or has closed, or its order under our id has another side or symbol | Find its source, such as another terminal or a late arrival. Cancel at the venue if unwanted. New orders are halted. |
| Break `orphan_fill` | The venue reports a fill on an order ismet never sent | Find its source. New orders are halted once per fill: a release is final, also across restarts. If the fill moved a position, record it with `ismet position adjust`, or `position_mismatch` halts again. |
| Break `position_mismatch` | Position differs from baseline plus fills and adjustments, for longer than `unknown_grace` | Look for outside trades, transfers, or corporate actions, then record the cause with `ismet position adjust`. New orders are halted. |
| Break `fill_mismatch` | The venue's filled quantity held still across a fill query, and that query did not explain it | Get the venue's execution report. New orders are halted. |
| Break `missing_at_venue` | A working order vanished, as can happen on venue failover | Confirm with the venue. The order is marked cancelled. New orders are halted. |
| Anomaly `overfill` | More filled than ordered, judged against the requested size while a replace is pending | Contact the venue. New orders are halted. |
| Anomaly `before the send started` | A venue report arrived for an order ismet never sent | The books were wrong. New orders are halted. |
| `JournalError` on submit | The journal cannot be written, so that order was refused and nothing was sent | Check disk space and permissions. Orders are accepted again once writes succeed. |
| `KillSwitchEngaged("journal failure: ...")` on submit | A venue event could not be journalled. It was applied in memory, and is rewritten in order once the journal recovers | Fix the disk, then restart: only a restart clears this. `ismet kill status` shows `not engaged` and `ismet kill release` cannot clear it, because it lives in this process. |
| `ConfigError` at start: risk limits differ | The latest journalled limits are not the ones passed in | Start with the journalled limits, or a person's earlier tightening is lost. Then change them with `update_risk_config`. |
| `RiskRejected` from check `max_gross_notional`, "gross exposure unknown" | Holdings, a mark, or a multiplier is unknown, or exposure spans currencies | Check the reference quote of every held symbol. Orders that only reduce a position still pass. Set the gross limit to `None` to trade several currencies, since no rate is assumed. |
| `RiskRejected` from check `limit_currency` | The order's currency is not `RiskConfig.limit_currency` | Limits are not converted. Trade that instrument under a configuration whose limits are in its currency. |
| `journal failed verification` at start | The audit trail was altered, truncated, or corrupted | Treat as an incident. Preserve the journal and its `.head` file, and do not delete them to get started. |
| `RiskRejected` from check `reconciliation` | The latest pass could not query the venue, so exposure is unverified | Fix connectivity. Orders resume by themselves after a clean pass, which retries every few seconds. |
| `RiskRejected` from check `execution_stream` | The execution stream is down | It reconnects by itself and reconciles. Investigate if it persists. |
| Halt after a bust | A fill was busted while ismet was down, on a venue that does not repeat the bust, so the books keep it and the venue does not | Confirm with the venue. ismet cannot tell a bust from a lost report, so it halts rather than guess. |
| `ReportQuarantinedError` from a FIX stream | A report could not be applied, or a sequence reset may have skipped reports. New orders stay refused, also across restarts | Compare with the venue's records, then call `await FixProvider.release_quarantine(operator=..., reason=...)`, which is recorded in the store. |
| `CalendarOutOfRange` rejection | The venue's calendar data has ended | Upgrade ismet, or supply newer data with `load_calendar_file`. `ismet doctor` warns 90 days ahead. |
| `stale_data` rejection | The reference quote is too old | Check the market data feed. |

## Monitoring

Bind `ismet.observability.Metrics` to your metrics system and alert on the following.

| Metric | Alert when |
|---|---|
| `ismet_recon_breaks_total{kind}` | Any increase; for `query_failed`, a sustained rate |
| `ismet_order_events_total{kind="outcome_unknown"}` | Any increase |
| `ismet_orders_terminal_total{status="rejected"}` | Rate above your normal level. An order counts once when it closes, under the status it closed with. |
| `ismet_http_requests_total{outcome="error"}` | Rate above your normal level |
| `ismet_http_latency_seconds` | p99 above your budget |

Also poll `OrderManager.stream_healthy` and `OrderManager.kill_state`.
Market data reconnects retry forever by default, and each logs a WARNING on `ismet.transport.ws`.
Pass `ws_on_state` to a provider to watch them, or `ws_max_reconnects` to fail instead.
A stream that silently stops delivering cannot be told apart from a quiet market, so periodic reconciliation (`reconcile_interval`, 30 seconds by default) is what bounds how long a missed report can go unnoticed.

## Journal care

- Keep the journal on a local disk with a real flush.
  Network filesystems can break SQLite's locking.
- Back it up with the SQLite backup API or `ismet journal export`, not by copying a live file.
- `ismet journal export "$JOURNAL" --out <new file>` refuses an existing file. `ismet.execution.journal.verify_jsonl` checks the hash chain of an export on its own.
- Copy `head_hash` from `ismet journal verify` to separate storage on a schedule, then check against it with `ismet journal verify "$JOURNAL" --expect-head "$SAVED"`.
  An empty value is a usage error, so an unset variable cannot skip the check.
- Journal commands refuse a file that holds no journal table, so a mistyped path cannot look like a successful halt.
- Retention is your responsibility.
  See [COMPLIANCE.md](COMPLIANCE.md).

## Clock discipline

Run NTP or PTP on the host.
`MarketEvent.clock_skew` measures the venue clock against the local one, and `QualityMonitor` flags excess skew.
ismet measures skew, and it does not discipline the clock.

## Calendar updates

```bash
ismet calendar XNSE
```

The `data_covers` line shows when the bundled data ends.
For an announced closure, build the closed calendar and register it:

```python
from datetime import date

from ismet.calendars import get_calendar, register_calendar

register_calendar(get_calendar("XNYS").with_closure(date(2026, 12, 7)))
```

`with_closure` only returns a copy, and `register_calendar` and `load_calendar_file` change the running process alone.
Apply them at startup in every process that uses the venue, including after each restart, until a release carries the closure.

## FIX sessions

`ismet.fix` is experimental, and these are the operational rules that differ from other providers.

- Credentials are refused without `tls = true`, or `allow_plaintext_credentials = true` when a stunnel or VPN protects the connection.
- `reset_on_logon` resets once per `FixSessionConfig.session_period`, by default the UTC date, and a reconnect within the period keeps its numbers.
  Set it and `session_period` to match the venue's daily reset.
  Otherwise the venue's reset arrives unrequested and halts the stream until released.
- The first reset of a period makes the prior period's messages unrequestable, so open orders are then caught only by the reconciliation CumQty comparison.
- A resend request retransmits a stored order under its original number with PossDupFlag=Y only within `stale_resend_after`, 60 seconds by default, and older application messages are gap-filled.
- Every stream subscribe repeats `FillBusted` for each busted execution in the store, and the order manager ignores a repeat.
