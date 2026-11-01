# ADR 0003: Bundle a calendar snapshot rather than depend on exchange_calendars

Status: accepted, 2026-11-01.

## Context

`exchange_calendars` is the best open source for this, but it requires pandas and numpy, which conflicts with a light pure-Python core.
Its future dates are projections from rules, or hand-maintained lists that end.

## Decision

Generate a compact JSON snapshot with `scripts/update_calendars.py`, and test in CI that it reproduces the source session by session.
Queries outside the data raise `CalendarOutOfRange` rather than guess.
NSE, Nasdaq, Shenzhen, and JPX reuse BSE, NYSE, Shanghai, and Tokyo data, and each alias is explained in `scripts/update_calendars.py`.

## Consequences

A snapshot goes stale, and a stale calendar is dangerous.
Coverage therefore ends visibly, `ismet doctor` warns 90 days ahead, and risk checks fail closed past the end, except for an order that rests for later (OPG, CLS, or GTC or GTD on a limit or stop limit), which skips the session check.
XBOM, XNSE, XSHG, XSHE, and XSES currently end on 2026-12-31 because the source's lists end there, so a data refresh is needed before that date.
Pre-open, auction, and extended sessions are not modelled.
