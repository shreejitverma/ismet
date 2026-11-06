# Regulatory Control Mapping

**ISMET does not make anyone compliant.**
It is software a regulated firm may use as one part of its own controls.
The firm remains responsible for its obligations, and this page is not legal advice.
Have your compliance function and counsel confirm each item.

Status of each source was checked on 2026-11-01, as noted below.
Rules change, so re-verify before relying on this page.

## US: SEC Rule 15c3-5, Market Access

Source: [17 CFR 240.15c3-5 (eCFR)](https://www.ecfr.gov/current/title-17/chapter-II/part-240/subject-group-ECFRc8401dcba174f73/section-240.15c3-5).

| Requirement | ISMET mechanism | Firm remains responsible for |
|---|---|---|
| (c)(1)(i) Credit and capital thresholds | `max_gross_notional` and `max_position`, by default and per symbol. Both are refused in per account and per strategy scopes, because exposure is tracked per symbol and not per scope. | Setting appropriate thresholds. Aggregating across every system, since ISMET sees only its own orders. Enforcing account or strategy exposure limits elsewhere. |
| (c)(1)(ii) Reject erroneous orders by price or size, per order or over a short period; catch duplicates | `price_collar_bps`, `max_order_quantity`, `max_order_notional`, `max_orders_per_second`, duplicate check | Calibrating parameters per instrument |
| (c)(2) Regulatory requirements before entry | Restricted list, short-sale marking and locate hook, session check | The content of those lists, and all other pre-entry regulatory requirements |
| (c)(2) Authorized access | Not provided | Access control to the systems running ISMET |
| (c)(2) Immediate post-trade reports to surveillance | `on_event` hook and journal | Building and staffing surveillance |
| (d) Direct and exclusive control of controls | Runs in the firm's own process | **Critical.** Using ISMET as a library may or may not satisfy this. It depends on who operates it and who controls its configuration. |
| (e) Regular review and annual CEO certification | Journal, tests, and these documents as supporting evidence | The review and the certification |

Rule 15c3-5 applies to broker-dealers with market access.
It does not apply directly to most buy-side users, but its checks are a sound baseline.

## US: other

- **Reg SHO**: `ShortSaleMark` carries sale marking, and `locate_check` is a hook for the firm's locate source.
  ISMET does not source locates, and it does not implement close-out requirements or the alternative uptick rule.
- **Books and records (Rule 17a-4)**: the journal is append-only with a hash chain.
  Rule 17a-4 has specific requirements for electronic recordkeeping systems, and this journal has not been assessed against them.
  Do not assume it qualifies.
- **CAT reporting**: not provided.
  The journal holds order lifecycle events a reporting pipeline could use.
- **Tick size and access fees (Rule 612 and Rule 610 amendments)**: secondary sources report that the SEC deferred compliance to the first business day of November 2027 ([summary](https://www.findknowdo.com/node/660563)), and further changes are possible alongside order protection rule work ([FlexTrade](https://flextrade.com/resources/secs-broader-reg-nms-overhaul-could-delay-access-fee-tick-sizes/)).
  **Unverified against the SEC order itself.**
  `TickLadder` models whichever tick regime applies, and no tick regime is hard-coded.

## EU: MiFID II algorithmic trading

Sources: [Delegated Regulation (EU) 2017/589, RTS 6](https://eur-lex.europa.eu/eli/reg_del/2017/589/oj/eng) and [Delegated Regulation (EU) 2017/574, RTS 25](https://eur-lex.europa.eu/eli/reg_del/2017/574/oj/eng).

| Requirement | ISMET mechanism | Firm remains responsible for |
|---|---|---|
| RTS 6 Art. 12: kill functionality to cancel unexecuted orders immediately | Kill switch across processes sharing a journal | Coverage across all venues and systems. ISMET covers orders through one order manager per provider. |
| RTS 6 Art. 15: price collars, maximum order value and volume, message limits | `price_collar_bps`, `max_order_notional`, `max_order_quantity`, `max_orders_per_second` | Calibration, and the governed override process for blocked orders, which ISMET deliberately does not offer |
| RTS 6: DEA clients' orders pass through the provider's controls | Not provided | The DEA provider's own control layer |
| RTS 25: UTC-synchronized clocks and timestamp granularity | Timestamps are UTC; `exchange_ts_ns` preserves nanoseconds; skew is measurable | Clock synchronization and traceability. ISMET does not discipline clocks. |

ESMA material refers to a revised RTS 25 and to its repeal under the MiFIR review.
**Current status was not confirmed**, so check it before relying on the above ([ESMA, MiFIR Art. 22c](https://www.esma.europa.eu/publications-and-data/interactive-single-rulebook/mifir/article-22c-synchronisation-business-clocks)).

DORA, MAR, and MiCA are not mapped here yet.

## India: SEBI retail algorithmic trading

Sources: SEBI's circular of 30 September 2025 ([PDF](https://www.sebi.gov.in/sebi_data/attachdocs/sep-2025/1759232056254.pdf)), which sets the framework of the 4 February 2025 circular as applicable to all stock brokers from 1 April 2026.

The framework's operational detail (exchange registration of algos, static IP, and algo identifiers) was seen only in secondary sources and is **unverified**.
Exchange and broker standards determine what an API client must send.
ISMET does not have an algo identifier field wired to any Indian broker yet, and it has no Indian broker adapter yet.
`OrderRequest.strategy` is the intended carrier once one exists.

## Market data licensing

The ISMET wheel and source distribution ship no market data and no redistribution rights.
The public source repository still contains recorded venue responses under `tests/fixtures/`, pending the legal review listed in `docs/GAP_MATRIX.md`.
Each provider's data is governed by the user's agreement with that provider.
Exchange data commonly restricts display, non-display use, and redistribution.
Calendars bundled with ISMET are factual schedules derived from an Apache-2.0 project.
ISO 10383 and ISO 4217 snapshots come from their registration authorities.

## Data retention

ISMET never deletes journal records.
Choosing and enforcing a retention period is the firm's responsibility.
