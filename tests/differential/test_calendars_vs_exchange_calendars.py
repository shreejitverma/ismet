"""Proves the bundled snapshot reproduces its source, session by session.

Runs where ``exchange_calendars`` is installed; CI has a dedicated job for it.
"""

from __future__ import annotations

import json
from importlib import resources

import pytest

ec = pytest.importorskip("exchange_calendars")

from ismet.calendars import get_calendar  # noqa: E402

SNAPSHOT = json.loads(
    resources.files("ismet.calendars")
    .joinpath("data")
    .joinpath("calendars.json")
    .read_text(encoding="utf-8")
)


@pytest.mark.parametrize("venue", sorted(SNAPSHOT["venues"]))
def test_every_session_matches_the_source(venue: str) -> None:
    source = ec.get_calendar(venue)
    ours = get_calendar(venue)
    theirs = source.sessions[source.sessions >= str(ours.first_date)]
    theirs = theirs[theirs <= str(ours.last_date)]
    start, end = theirs[0].date(), theirs[-1].date()
    sessions = ours.sessions(start, end)
    assert len(sessions) == len(theirs)
    breaks = source.break_starts is not None
    for session, label in zip(sessions, theirs, strict=True):
        assert session.date == label.date()
        assert session.open == source.session_open(label).to_pydatetime()
        assert session.close == source.session_close(label).to_pydatetime()
        if breaks:
            start_break = source.break_starts.loc[label]
            if start_break == start_break:  # NaT compares unequal to itself
                assert session.break_start == start_break.to_pydatetime()
                assert session.break_end == source.break_ends.loc[label].to_pydatetime()
            else:
                assert session.break_start is None
