"""Re-run historical failure scenarios unchanged under deterministic clock faults."""

import pytest
import test_delivery_observation as observation_scenarios
import test_ingestion_polling_db as polling_scenarios
import test_knowledge_delivery_db as delivery_scenarios
from test_delivery_clock_regression_db import (
    shift_first_release,
    shifted_database_clock,
)
from test_durable_delivery_db import db as db

from app.infrastructure.messaging.durable_delivery import DurableDelivery


@pytest.mark.parametrize("fault", ["upload", "parse"])
async def test_response_loss_retry_after_release_clock_regression(
    db, monkeypatch, fault
):
    releases = shift_first_release(db, monkeypatch)
    await delivery_scenarios.test_response_loss_recovers_without_repeating_remote_write(
        db, monkeypatch, fault
    )
    assert len(releases) == 2


async def test_poll_after_previous_step_release_clock_regression(db, monkeypatch):
    releases = shift_first_release(db, monkeypatch)
    await polling_scenarios.test_hung_provider_read_is_cancelled_at_deadline(
        db, monkeypatch
    )
    assert len(releases) == 2


async def test_replay_observation_after_clock_regression(db, monkeypatch):
    original = DurableDelivery.replay
    calls = []

    async def replay(self, identifier):
        calls.append(identifier)
        with shifted_database_clock(db, seconds=3600):
            await original(self, identifier)

    monkeypatch.setattr(DurableDelivery, "replay", replay)
    await observation_scenarios.test_expired_publisher_lease_and_dead_letter_replay(db)
    assert len(calls) == 1
