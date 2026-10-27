"""Whole-feature scheduling within the integration runner's slot budget."""

import pytest

from tests.integration.feature_queue import Feature, FeatureQueue


def _feature(name, scenarios, *tags):
    return Feature(name, scenarios, scenarios, frozenset(tags))


def test_heavy_features_start_early_and_share_spare_slots():
    large = _feature("large", 40)
    medium = _feature("medium", 30)
    restore = _feature("restore", 24, "parallel_heavy", "dependent-scenarios")
    freeze = _feature("freeze", 2, "parallel_heavy")
    queue = FeatureQueue([large, medium, restore, freeze], jobs=3)

    assert queue.take() == restore
    assert queue.take() == large
    assert queue.take() is None

    # Finishing the first heavy feature must admit the next one before medium.
    queue.finish(restore, True)
    assert queue.take() == freeze
    assert queue.take() is None
    queue.finish(large, True)
    assert queue.take() == medium
    queue.finish(freeze, True)
    queue.finish(medium, True)
    assert queue.take() is None


@pytest.mark.parametrize("jobs", [1, 2, 3, 6])
def test_priority_respects_slot_budget_and_exclusive_features(jobs):
    normal = _feature("normal", 40)
    heavy = _feature("heavy", 2, "parallel_heavy")
    exclusive = _feature("exclusive", 50, "parallel_exclusive")
    queue = FeatureQueue([normal, heavy, exclusive], jobs)

    assert queue.take() == exclusive
    assert queue.take() is None
    queue.finish(exclusive, True)
    first, second = (normal, heavy) if jobs == 1 else (heavy, normal)
    assert queue.take() == first
    if jobs <= 2:
        assert queue.take() is None
        queue.finish(first, True)
    assert queue.take() == second
    assert sum(feature.slots(jobs) for feature in queue.active.values()) <= jobs


def test_equal_priorities_keep_featureset_order():
    first = _feature("first", 2, "parallel_heavy")
    second = _feature("second", 2, "parallel_heavy")
    queue = FeatureQueue([first, second], jobs=3)
    assert queue.take() == first
    queue.finish(first, True)
    assert queue.take() == second


def test_failure_prevents_starting_pending_heavy_features():
    first = _feature("first", 3, "parallel_heavy")
    second = _feature("second", 2, "parallel_heavy")
    queue = FeatureQueue([first, second], jobs=3)
    assert queue.take() == first
    queue.finish(first, False)
    assert queue.take() is None
    assert queue.pending == [second]
