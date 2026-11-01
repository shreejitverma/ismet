# ADR 0002: Journal first, send once, resolve unknowns by asking

Status: accepted, 2026-11-01.

## Context

The most expensive failure in order entry is not knowing what happened.
A timeout does not mean the order failed.
Retrying creates duplicates, and assuming failure leaves unmanaged risk.

## Decision

- Write the intent, then a `send_started` record, durably, before the network call.
- Send once, and never retry automatically.
- A failure that may have delivered becomes `UNKNOWN`, which blocks new orders in that symbol and is resolved only by querying the venue by client order id.
- Before declaring an order lost, wait out a grace period and send a cancel by client order id, to catch a late arrival.
- The order's state is a pure function of its events, and a contradictory venue report is recorded and surfaced, never dropped.

## Alternatives rejected

- Automatic retry with an idempotency key: not every venue supports one, and venues differ on how long keys last.
  Adapters may add it inside `place_order`.
- A separate OMS service: a stronger isolation boundary, but it would remove the "library you can import" property, which is what makes this useful to smaller firms.

## Consequences

Each order costs two durable writes before it leaves, about 10 ms on the machine in `docs/BENCHMARKS.md`.
This suits strategies measured in tens of orders per second, not colocated market making.
