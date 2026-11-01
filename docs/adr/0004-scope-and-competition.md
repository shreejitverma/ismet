# ADR 0004: Scope against existing tools

Status: accepted, 2026-11-01.
Comparisons reflect project documentation as understood on this date, and should be rechecked.

| Tool | What it does well | Position |
|---|---|---|
| ccxt | Breadth of crypto venues | Do not race on venue count. Compete on typed models, exact money, and order safety. |
| NautilusTrader | Rust core, backtest and live parity | Do not build a backtester. Offer data and execution bridges. |
| QuantConnect LEAN | Full research and live platform | Integrate, do not compete. |
| ib_async | Deep Interactive Brokers API | A likely foundation for a future IBKR adapter. |
| OpenBB | Research data aggregation | Different audience. ISMET targets execution-grade correctness. |
| exchange_calendars | Calendar data | Adopted as the snapshot's source. |
| QuantLib | Pricing | Integrate, do not reimplement. |

## Decision

ISMET owns the safety-critical layer between Python and a venue.
It will not build alpha models, a portfolio optimizer, an exotic pricing library, a charting terminal, data redistribution, or a research backtester.

## Consequences

Users still need other tools for research.
The product stays small enough to be verified, which is the point.
