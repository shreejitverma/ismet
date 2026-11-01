"""The order path end to end against the simulated venue. No account or network.

Shows what the order manager is for: a risk rejection, a fill, a lost response that
is resolved by asking the venue rather than by resending, and the kill switch.
"""

import asyncio
import tempfile
from decimal import Decimal
from pathlib import Path

from ismet import (
    KillSwitchEngaged,
    OrderManager,
    OrderRequest,
    OrderSide,
    RiskConfig,
    RiskEngine,
    RiskLimits,
    RiskRejected,
    SqliteJournal,
    Symbol,
)
from ismet.testing.simvenue import SIM_VENUE, Fault, SimVenue, sim_instrument

ACME = Symbol(venue=SIM_VENUE, ticker="ACME")


def buy(client_order_id: str, quantity: str, price: str) -> OrderRequest:
    return OrderRequest(
        client_order_id=client_order_id,
        symbol=ACME,
        side=OrderSide.BUY,
        quantity=quantity,
        limit_price=price,
    )


async def main(journal_path: Path) -> None:
    venue = SimVenue(instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "99.99", "100.01")
    risk = RiskEngine(
        RiskConfig(
            default=RiskLimits(
                max_order_quantity=Decimal(1000),
                max_order_notional=Decimal(100_000),
                max_position=Decimal(5000),
                price_collar_bps=Decimal(100),
            )
        )
    )
    journal = SqliteJournal(journal_path)
    async with OrderManager(venue, journal, risk, unknown_grace=0.0) as oms:
        try:
            await oms.submit(buy("fat-finger", "100", "150.00"))
        except RiskRejected as exc:
            print(f"refused before sending: {exc.check}: {exc.reason}")

        await oms.submit(buy("marketable", "10", "100.01"))
        filled = await oms.wait_for("marketable", lambda o: o.is_terminal, 5)
        print(f"{filled.status.value} at {filled.average_price}")

        venue.faults.append(Fault.LOSE_RESPONSE)
        venue.report_faults.drop_next = 1
        state = await oms.submit(buy("lost-ack", "5", "99.00"))
        print(f"response lost, status is {state.status.value}")
        await oms.reconcile()
        print(f"after asking the venue: {oms.order('lost-ack').status.value}")
        print(f"times sent: {venue.attempts['lost-ack']}")

        await oms.engage_kill("end of demo", operator="example")
        try:
            await oms.submit(buy("after-kill", "1", "99.00"))
        except KillSwitchEngaged as exc:
            print(f"refused: {exc.reason}")
        settled = await oms.wait_for("lost-ack", lambda o: o.is_terminal, 5)
        print(f"open order cancelled by the switch: {settled.status.value}")
        print(f"position from fills: {oms.net_position(ACME)}")
    print(f"journal chain intact: {journal.verify().ok}")
    journal.close()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        asyncio.run(main(Path(directory) / "orders.db"))
