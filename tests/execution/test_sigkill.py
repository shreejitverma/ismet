"""A real process death: SIGKILL, with no chance to run any cleanup.

The child runs an order manager against a venue whose every accepted order is appended
to a file that outlives the child, standing in for a real exchange. The parent then
rebuilds that venue from the file, restarts on the same journal, and checks the books.
"""

from __future__ import annotations

import json
import os
import random
import signal
import subprocess
import sys
import textwrap
import time
from decimal import Decimal
from pathlib import Path

import pytest

from ismet.execution import (
    OrderManager,
    RiskConfig,
    RiskEngine,
    RiskLimits,
    SqliteJournal,
)
from ismet.models.orders import OrderRequest, OrderStatus
from ismet.testing.simvenue import SimVenue, sim_instrument

from .conftest import ACME

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="SIGKILL does not exist on Windows"
)

CHILD = textwrap.dedent(
    """
    import asyncio, os, signal, sys
    from decimal import Decimal
    from ismet.execution import (
        OrderManager, RiskConfig, RiskEngine, RiskLimits, SqliteJournal,
    )
    from ismet.models import Symbol
    from ismet.models.orders import OrderRequest, OrderSide
    from ismet.testing.simvenue import SimVenue, sim_instrument

    journal_path, log_path, kill_step = sys.argv[1], sys.argv[2], sys.argv[3]
    ACME = Symbol(venue="ZSIM", ticker="ACME")

    class DurableVenue(SimVenue):
        async def place_order(self, request):
            with open(log_path, "a") as log:
                log.write(request.model_dump_json() + "\\n")
                log.flush()
                os.fsync(log.fileno())
            return await super().place_order(request)

    def die(step, cid):
        if step == kill_step:
            os.kill(os.getpid(), signal.SIGKILL)

    async def main():
        venue = DurableVenue(instruments=[sim_instrument("ACME")])
        venue.set_quote(ACME, "9.99", "10.01")
        risk = RiskEngine(RiskConfig(
            default=RiskLimits(max_order_quantity=Decimal(1000)),
            duplicate_window_seconds=Decimal(0),
            prevent_self_match=False,
        ))
        oms = OrderManager(venue, SqliteJournal(journal_path), risk, checkpoint=die)
        await oms.start()
        await venue.drain()
        print("ready", flush=True)
        number = 0
        while True:
            number += 1
            await oms.submit(OrderRequest(
                client_order_id=f"K{number}", symbol=ACME, side=OrderSide.BUY,
                quantity="1", limit_price="9.00",
            ))

    asyncio.run(main())
    """
)


def run_child(journal: Path, log: Path, step: str, tmp: Path) -> subprocess.Popen[str]:
    script = tmp / "child.py"
    script.write_text(CHILD, encoding="utf-8")
    return subprocess.Popen(
        [sys.executable, str(script), str(journal), str(log), step],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )


def rebuilt_venue(log: Path) -> tuple[SimVenue, list[str]]:
    venue = SimVenue(instruments=[sim_instrument("ACME")])
    venue.set_quote(ACME, "9.99", "10.01")
    accepted: list[str] = []
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            request = OrderRequest.model_validate(json.loads(line))
            venue.add_external_order(request)
            accepted.append(request.client_order_id)
    return venue, accepted


async def recover_and_check(journal_path: Path, log: Path) -> None:
    venue, accepted = rebuilt_venue(log)
    assert len(accepted) == len(set(accepted)), "an order reached the venue twice"
    journal = SqliteJournal(journal_path)
    assert journal.verify().ok, "a kill left the journal chain broken"
    risk = RiskEngine(
        RiskConfig(default=RiskLimits(), duplicate_window_seconds=Decimal(0))
    )
    oms = OrderManager(
        venue,
        journal,
        risk,
        unknown_grace=0.0,
        reconcile_interval=3600,
        stream_settle=None,
    )
    report = await oms.start()
    try:
        await oms.reconcile()
        assert set(accepted) <= set(oms.orders), (
            "the venue has an order the journal lost"
        )
        for cid in accepted:
            assert oms.order(cid).status is OrderStatus.ACCEPTED
        for order in oms.open_orders():
            assert order.client_order_id in accepted
        assert not any(b.is_critical for b in report.breaks)
        assert not oms.kill_state.engaged
        assert not venue.attempts  # recovery itself never sent anything
    finally:
        await oms.abort()
        journal.close()


@pytest.mark.parametrize(
    "step", ["after_intent", "after_send_started", "after_venue_accept"]
)
async def test_sigkill_at_each_step_of_the_send_path(tmp_path: Path, step: str) -> None:
    journal, log = tmp_path / "orders.db", tmp_path / "venue.log"
    child = run_child(journal, log, step, tmp_path)
    child.wait(timeout=60)
    child.stdout.close()  # type: ignore[union-attr]
    assert child.returncode == -signal.SIGKILL
    await recover_and_check(journal, log)


@pytest.mark.parametrize("seed", range(6))
async def test_sigkill_at_a_random_moment_including_mid_commit(
    tmp_path: Path, seed: int
) -> None:
    journal, log = tmp_path / "orders.db", tmp_path / "venue.log"
    child = run_child(journal, log, "never", tmp_path)
    assert child.stdout is not None
    assert child.stdout.readline().strip() == "ready"
    deadline = time.monotonic() + 30
    while not (log.exists() and log.stat().st_size):
        assert time.monotonic() < deadline, "the child never sent an order"
        time.sleep(0.001)
    time.sleep(random.Random(seed).uniform(0.01, 0.25))
    os.kill(child.pid, signal.SIGKILL)
    child.wait(timeout=30)
    child.stdout.close()
    assert child.returncode == -signal.SIGKILL
    probe = SqliteJournal(journal)
    assert any(r.unique_intent for r in probe.records())
    probe.close()
    await recover_and_check(journal, log)
