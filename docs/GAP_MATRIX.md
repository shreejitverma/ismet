# Gap Matrix

The honest status of every requirement in [MASTER_PROMPT.md](MASTER_PROMPT.md).
Update it in the same change as the work.
Last assessed on 2026-11-08 at version 0.4.0.

**Overall: P0 is done, P3's stated scope and exit are met with its optional order types still missing, P1, P2, and P5 are partial, and P4 and P6 have not started.**
P5 work began before P4's evidence exists, which departs from the brief's phase order: P4 is blocked on accounts, and the FIX engine and audit work did not need it.
ISMET has never placed an order at a real broker, so it is not production-ready.

| Status | Meaning |
|---|---|
| Proven | Built, with tests that fail if it breaks, confirmed by deliberate mutation where noted |
| Built | Exists and is tested, with a named limit on the evidence |
| Partial | Some of the requirement exists |
| Missing | Does not exist |
| Blocked | Needs something only a person can provide |

## Evidence behind "Proven"

- Crash safety: tests in `tests/execution/test_sigkill.py` deliver real `SIGKILL` at each send step and at random moments, and a matrix of simulated crashes covers network faults.
- Tests that catch regressions: 54 hand-written mutants of the safety-critical code in `mutation/mutants.toml`, covering the order manager, kill switch, journal, order state, pre-trade risk, HTTP retry rules, the FIX session and provider, cancellation, and decimal arithmetic.
  All 54 were killed on 2026-11-08 by `uv run python scripts/mutation_check.py`, which takes about 4 minutes.
  Five first survived, and four new tests now kill them.
  A weekly CI workflow is set to repeat the run but has not run yet, and a unit test fails when a refactor stops a mutant from applying.
- Independent review: three fresh-context reviews reproduced about 100 defects in the first version of the core, each fixed with a regression test. A later review covered `ismet.fix`, the adapters, and the rest of the change, by reading and tracing, and its findings were fixed in the same change with regression tests. No second review has judged those fixes.

Mutants are hand-picked, so "54 of 54" says the listed risks are covered, not that all behaviour is.
An earlier, uncommitted set of 88 mutants is retired; the committed catalogue replaces it and anyone can rerun it.

## Known limits

- A lost FIX 150=G before a crash can cause a false overfill halt on restart: the first reconciliation runs before the stream replays busts, so it books the corrected fill while the old one still counts. This fails closed, and clears after an operator release.
- The first FIX sequence reset of a session period makes the prior period's messages unrequestable. With an order working and a previous connection that did not end with a clean Logout exchange, the reset halts through the quarantine until an operator releases it. After a clean Logout, a report the venue sent while we were logged out is caught only by the reconciliation CumQty comparison on an open order.
- A FIX SequenceReset-GapFill over a report the venue no longer holds is not visible when it arrives. The next report on that order whose CumQty differs from the fills received, below for a lost bust or correction and above for a lost fill, halts through the quarantine and ends in a `FILL_MISMATCH` break. With no later report, only reconciliation of an open order can catch it.
- XBOM, XSHG, and XSES data, and the aliases XNSE and XSHE, ends on 2026-12-31, and orders on those venues are then refused unless they rest for later.
  The newest exchange_calendars (4.13.2, checked 2026-11-07) still ends there, so no release can extend it yet; an operator extends a venue from its own holiday circular with `TradingCalendar.extended_through`, as docs/OPERATIONS.md describes.
- The bundled calendars hold no weekend session for any venue, XBOM included: every venue's `extra` list is empty and no `special` date falls on a weekend.
  A weekend session a venue really holds therefore reads as closed, and whether XBOM holds any has not been checked.
- Conformance does not check for open bars.
  It asks for a range ending at the last whole minute, so a provider returning a partial bar is never exposed, and a range past now needs newly recorded cassettes.
- Conformance checks that bars are a whole number of intervals apart, not that none are missing.
  Bars of a longer interval mislabelled as the asked one, such as one bar every 30 minutes labelled 1m, still pass, because a venue may close or have minutes with no trades.
- The private code for the simulated venue changed from `XSIM`, which is a real MIC, to `ZSIM`.

## P0 Correctness

| Requirement | Status | Evidence |
|---|---|---|
| Clock skew, locked and crossed markets, zero and negative prices are valid data | Proven | `tests/unit/test_correctness_regressions.py` |
| Exact decimal arithmetic, independent of ambient context | Proven | same; ADR 0001 |
| ISO MIC and currency validation; ISIN, CUSIP, SEDOL, FIGI, LEI check digits | Proven | same; snapshots from the publishers |
| No resend of a non-idempotent request | Proven | same |
| Nanosecond timestamps, tick ladders, session-based daily bars | Proven | same |
| Sequence gaps reach the consumer | Proven | `tests/marketdata/test_quality.py` |

## P1 Foundations

| Requirement | Status | Note |
|---|---|---|
| Domain model: money conventions | Partial | `QuoteConvention` and minor-unit codes exist. Bond clean and dirty pricing and accrual are not implemented. |
| Contract specs | Partial | Option and future terms are validated. Chains and continuous contracts are missing. |
| Point-in-time security master | Built | `SecurityMaster` works but ships empty: no vendor feeds it yet. |
| Corporate actions | Built | Split and dividend back-adjustment. A dividend needs the bar of the session before its ex-date, or an explicit `price_factor`. Rights, spin-offs, and mergers need an explicit factor. No vendor feeds it yet. |
| Calendars | Built | 24 venues and 4 aliases, checked against the source (4.13.2 in CI, the newest weekly). XNSE, XBOM, XSHG, XSHE, XSES data ends 2026-12-31, and an operator can extend a venue from its published holiday list. Aliases are stated assumptions, and are not checked against the source. |
| Pre-open, auction, extended sessions | Missing | Regular hours and breaks only. |
| Data quality monitor | Built | Not wired into streams by default; the caller runs it. |
| Conformance v2 | Proven | Market data and trading suites. Tests prove the trading suite catches broken adapters. |

## P2 Market data

| Requirement | Status | Note |
|---|---|---|
| Binance, Coinbase adapters | Built | Public data only, on recorded fixtures and one manual live run. Reviewed by reading. Coinbase bars from an outdated CDN copy are flagged `STALE`; the `Date` and `Age` headers decide that, and only a later request can do better. |
| Polygon, Alpaca, Kite, Interactive Brokers | Blocked | Needs accounts or API keys to record real fixtures. Not started. |
| Sync facade, CLI | Proven | Tested through the real entry points. |
| Pandas and Polars export | Built | Decimals stay exact by default. |
| Book builder | Proven | Not yet used by an adapter's stream. |
| L3, auctions, imbalances, greeks, funding, open interest | Missing | Models and adapters do not exist. |
| Gap recovery on streams | Partial | Gaps are reported and flagged. Lost messages are not backfilled. On Binance and Coinbase, which declare `gates_streams`, the adapter's flags decide what is a gap, because venue sequence numbers are sparse, so `on_gap` reports `missing=0` there, and the first quote after a reconnect counts as a gap. |
| Bitemporal bar store, backfill accounting | Proven | SQLite. A Parquet store is not built; export through dataframes. |
| Generated adapter matrix, docs site | Missing | The README table is hand-written. |

## P3 Execution safety

| Requirement | Status | Note |
|---|---|---|
| Order state machine | Proven | Exhaustive table, stateful property test. |
| Durable journal, crash recovery | Proven | Real `SIGKILL`. Only a process kill was tested, not a power cut, so flush behaviour rests on SQLite and the drive. |
| Pre-trade risk | Proven | Fails closed. Locates are a caller-supplied hook. |
| Kill switch | Proven | Across processes. Limit: if the journal is down when it is engaged and the process then stops, the engagement is lost, and this is logged at critical level. |
| Simulated venue | Built | Faults modelled on broker behaviour from general knowledge, not validated against any broker. |
| Reconciliation | Proven | Orders, fills including closed orders, positions. The sweep window is durable. |
| Gross exposure | Built | Current marks refreshed per order, never cost basis. Fails closed across currencies, since no rate table exists. |
| Brackets, OCO, multi-leg | Missing | Not in the brief's P3 scope, listed for honesty. |
| Client-side order emulation | Missing | Deliberately absent. |

## P4 Paper trading

| Requirement | Status | Note |
|---|---|---|
| Round trips on Tier 1 sandbox or paper accounts | Blocked | Needs accounts. |
| Two-week soak | Blocked | Needs the above. |

## P5 Institutional

| Requirement | Status | Note |
|---|---|---|
| FIX 4.2, 4.4, 5.0 SP2 session and order entry | Partial | **Experimental.** 522 tests against a Python test acceptor, plus literal frames from the specification. A real QuickFIX acceptor interop run (44 passed, 1 skipped) was repeated by hand on 2026-11-06 against this change; QuickFIX is not in CI, so a later change can break interop unseen. No real counterparty. Reviewed by reading only. Open orders and fills come from the local record, so outside orders are unseen, and the session's own fills are missing too if their reports never arrived before a reset. A provider with no account API has no position check to catch that. Trading conformance passes with one declared exception, `run_trading_conformance(unknown_status_error=TransportError)`: an order with no local row raises rather than looking dead. The order manager reads that as not found only when the store id journalled with the send equals the current store's, since then the missing row proves the order never left. After a lost store the order stays unknown until an operator resolves it. |
| ITCH, OUCH, SBE parsers | Missing | |
| Audit trail | Proven | Hash chain, append-only triggers, anchor file. Not assessed against Rule 17a-4. |
| Control mappings | Partial | SEC 15c3-5, Reg SHO, MiFID II RTS 6 and RTS 25, SEBI, with unverified items marked. DORA, MAR, MiCA, and CAT are not mapped. |
| Metrics | Built | Protocol and hooks, and an OpenTelemetry adapter in the `otel` extra, tested against the SDK's in-memory reader. Not run against a real collector. |
| Runbook | Built | Not yet tested by a person other than its author. |
| Threat model, supply chain | Built | The CI matrix, CodeQL, and Dependabot have run; the release workflow, with attestation, SBOM, and the release gate, has not, since no version tag has been pushed. Action commits were checked once against upstream tags with `git ls-remote`, which shows the tag points at the commit and says nothing about its contents. |
| Vault, KMS, keychain, rotation | Missing | |
| Performance gates | Built | Generous CPU budgets. Group commit is not built. |
| Post-trade accounting | Built | Lots, P&L, per-currency fees, slippage. `Valuation.fills_with_unknown_fee` says when fees are a lower bound. `AVERAGE` lots round at the decimal context, so their P&L is not exact. Exchange taxes such as STT and settlement cash are not modelled. |
| Allocations, full TCA | Missing | |

## P6 1.0

| Requirement | Status | Note |
|---|---|---|
| Tier 2 providers | Missing | |
| Provider template, migration guides | Missing | |
| External security review | Blocked | Needs a person. |
| Three design partners | Blocked | Needs real users. |
| Legal and licensing review | Blocked | Needs counsel. |

## What is needed from a person

1. Paper or sandbox API keys for Alpaca, Interactive Brokers, Zerodha Kite, and Polygon.
2. A FIX counterparty's test environment.
3. An external security review, design partners, and a legal review of licensing and disclaimers.
