# Operations Runbook

Commands assume `JOURNAL` is the path of the order journal.

## Emergency: stop trading now

```bash
ismet kill engage --journal "$JOURNAL" --reason "<why>" --operator "<you>"
ismet kill status --journal "$JOURNAL"
```

Every order manager using this journal refuses new orders on its next attempt.
Each one also cancels open orders within `kill_poll_interval` (0.5 seconds by default) and keeps retrying until nothing is open.
Add `--keep-orders` to stop new orders but leave working orders alone.

A process that is not running cannot cancel anything.
If the order manager is down, cancel at the venue directly, then reconcile on restart.

## Resume after a halt

1. Find out why it stopped: `ismet kill status --journal "$JOURNAL"`.
2. Explain every reconciliation break before releasing.
3. Release under your own name: `ismet kill release --journal "$JOURNAL" --reason "<why>" --operator "<you>"`.

## Restart after a crash

Start the order manager on the same journal.
It takes these steps by itself, and you do not need to edit anything:

1. It verifies the hash chain, and refuses to start if it is broken.
2. It replays the journal.
3. It closes orders that never reached the send step as `NEVER_SENT`.
4. It asks the venue about anything that may have been sent, by client order id.
5. It recovers fills made while it was down.
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
| Break `orphan_order` | The venue has an open order ismet did not send or has closed | Find its source, such as another terminal or a late arrival. Cancel at the venue if unwanted. New orders are halted. |
| Break `position_mismatch` | Position differs from baseline plus fills | Look for outside trades, transfers, or corporate actions. Correct the cause. New orders are halted. |
| Break `fill_mismatch` | The venue reports a different filled quantity, and its fill query did not explain it | Get the venue's execution report. New orders are halted. |
| Break `missing_at_venue` | A working order vanished, as can happen on venue failover | Confirm with the venue. The order is marked cancelled. |
| Anomaly `overfill` | More filled than ordered | Contact the venue. New orders are halted. |
| `JournalError` on submit | The journal cannot be written, so nothing was sent | Check disk space and permissions. New orders stay blocked until restart. |
| `journal failed verification` at start | The audit trail was altered, truncated, or corrupted | Treat as an incident. Preserve the journal and its `.head` file, and do not delete them to get started. |
| `RiskRejected` from check `reconciliation` | The latest pass could not query the venue, so exposure is unverified | Fix connectivity. Orders resume by themselves after a clean pass, which retries every few seconds. |
| `RiskRejected` from check `execution_stream` | The execution stream is down | It reconnects by itself and reconciles. Investigate if it persists. |
| Halt after a bust | A fill was busted while ismet was down, so the books keep it and the venue does not | Confirm with the venue. ismet cannot tell a bust from a lost report, so it halts rather than guess. |
| `CalendarOutOfRange` rejection | The venue's calendar data has ended | Upgrade ismet, or supply newer data with `load_calendar_file`. `ismet doctor` warns 90 days ahead. |
| `stale_data` rejection | The reference quote is too old | Check the market data feed. |

## Monitoring

Bind `ismet.observability.Metrics` to your metrics system and alert on the following.

| Metric | Alert when |
|---|---|
| `ismet_recon_breaks_total{kind}` | Any increase; for `query_failed`, a sustained rate |
| `ismet_order_events_total{kind="outcome_unknown"}` | Any increase |
| `ismet_orders_terminal_total{status="rejected"}` | Rate above your normal level |
| `ismet_http_requests_total{outcome="error"}` | Rate above your normal level |
| `ismet_http_latency_seconds` | p99 above your budget |

Also poll `OrderManager.stream_healthy` and `OrderManager.kill_state`.
A stream that silently stops delivering cannot be told apart from a quiet market, so periodic reconciliation (`reconcile_interval`, 30 seconds by default) is what bounds how long a missed report can go unnoticed.

## Journal care

- Keep the journal on a local disk with a real flush.
  Network filesystems can break SQLite's locking.
- Back it up with the SQLite backup API or `ismet journal export`, not by copying a live file.
- Copy `head_hash` from `ismet journal verify` to separate storage on a schedule.
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
For an announced closure, apply `calendar.with_closure(day)` and register the result with `register_calendar`, which takes effect immediately without a release.
