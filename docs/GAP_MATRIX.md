# Gap Matrix

The honest status of every requirement in [MASTER_PROMPT.md](MASTER_PROMPT.md).
Update it in the same change as the work.
Last assessed on 2026-11-01 at version 0.4.0.

**Overall: P0 and P3 are done, P1, P2, and P5 are partial, and P4 and P6 have not started.**
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
- Tests that catch regressions: 88 hand-written mutants of the safety-critical code. 87 are caught, and the survivor (removing an early return in fill deduplication) leaves behaviour identical.
- Independent review: three fresh-context reviews reproduced about 100 defects in the first version of the core, each fixed with a regression test. They did not cover `ismet.fix` or the Binance and Coinbase adapters.

Mutants are hand-picked, so "87 of 88" says the listed risks are covered, not that all behaviour is.

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
| Corporate actions | Built | Split and dividend back-adjustment. Rights, spin-offs, and mergers need an explicit factor. No vendor feeds it yet. |
| Calendars | Built | 24 venues and 4 aliases, checked against the source. XNSE, XBOM, XSHG, XSHE, XSES data ends 2026-12-31. Aliases are stated assumptions. |
| Pre-open, auction, extended sessions | Missing | Regular hours and breaks only. |
| Data quality monitor | Built | Not wired into streams by default; the caller runs it. |
| Conformance v2 | Proven | Market data and trading suites. Tests prove the trading suite catches broken adapters. |

## P2 Market data

| Requirement | Status | Note |
|---|---|---|
| Binance, Coinbase adapters | Built | Public data only, on recorded fixtures and one manual live run. Not independently reviewed. |
| Polygon, Alpaca, Kite, Interactive Brokers | Blocked | Needs accounts or API keys to record real fixtures. Not started. |
| Sync facade, CLI | Proven | Tested through the real entry points. |
| Pandas and Polars export | Built | Decimals stay exact by default. |
| Book builder | Proven | Not yet used by an adapter's stream. |
| L3, auctions, imbalances, greeks, funding, open interest | Missing | Models and adapters do not exist. |
| Gap recovery on streams | Partial | Gaps are reported and flagged. Lost messages are not backfilled. |
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
| Reconciliation | Proven | Orders, fills including closed orders, positions. |
| Brackets, OCO, multi-leg | Missing | |
| Client-side order emulation | Missing | Deliberately absent. |

## P4 Paper trading

| Requirement | Status | Note |
|---|---|---|
| Round trips on Tier 1 sandbox or paper accounts | Blocked | Needs accounts. |
| Two-week soak | Blocked | Needs the above. |

## P5 Institutional

| Requirement | Status | Note |
|---|---|---|
| FIX 4.2, 4.4, 5.0 SP2 session and order entry | Built | **Experimental.** 423 tests, and a real QuickFIX acceptor interop run (44 passed, 1 skipped), run by hand because QuickFIX is not in CI. No real counterparty, and not independently reviewed. Open orders and fills come from the local record, so outside orders are unseen. |
| ITCH, OUCH, SBE parsers | Missing | |
| Audit trail | Proven | Hash chain, append-only triggers, anchor file. Not assessed against Rule 17a-4. |
| Control mappings | Partial | SEC 15c3-5, Reg SHO, MiFID II RTS 6 and RTS 25, SEBI, with unverified items marked. DORA, MAR, MiCA, and CAT are not mapped. |
| Metrics | Built | Protocol and hooks. OpenTelemetry extra is missing. |
| Runbook | Built | Not yet tested by a person other than its author. |
| Threat model, supply chain | Built | CI workflows have not run yet, including attestation and SBOM. |
| Vault, KMS, keychain, rotation | Missing | |
| Performance gates | Built | Generous CPU budgets. Group commit is not built. |
| Post-trade accounting | Built | Lots, P&L, per-currency fees, slippage. Exchange taxes such as STT and settlement cash are not modelled. |
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
