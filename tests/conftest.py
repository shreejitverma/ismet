from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ismet.models import Symbol

UTC = timezone.utc


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 8, 29, 12, 0, tzinfo=UTC)


@pytest.fixture
def sym() -> Symbol:
    return Symbol(venue="XMOK", ticker="ACME")


D = Decimal
