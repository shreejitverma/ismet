# ADR 0001: Exact decimal arithmetic in a private context

Status: accepted, 2026-11-01.

## Context

`Decimal` alone does not make money safe.
Arithmetic uses a thread-local context defaulting to 28 significant digits, which any library may change.
A crypto notional exceeds that, so `a + b` rounds silently and `a // b` raises `DivisionImpossible`.

## Decision

Money arithmetic goes through `ismet.models.decimals`, which uses a private 512-digit context that traps `Inexact`.
`to_decimal` bounds input to 64 significant digits and exponents within 64, so the private context can never overflow, and a hostile `1E+1000000` is rejected.
Only `ratio` rounds, explicitly, for values such as average price.
Its operands stay the source of truth.

## Consequences

Each operation costs about a microsecond more, as measured in `docs/BENCHMARKS.md`.
A value outside the bounds is rejected, not rounded.
