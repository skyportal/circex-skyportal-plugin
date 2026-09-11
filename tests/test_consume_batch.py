"""What a poll commits, so a failed circular is not buried by a later success."""

import json

import pytest

import main


class FakeMessage:
    def __init__(self, circular_id, offset, partition=0):
        self._value = json.dumps({"circularId": circular_id}).encode()
        self._offset = offset
        self._partition = partition

    def error(self):
        return None

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def topic(self):
        return "gcn.circulars"


class FakeConsumer:
    def __init__(self):
        self.committed = []
        self.sought = []

    def commit(self, message):
        self.committed.append(message.offset())

    def seek(self, tp):
        self.sought.append(tp.offset)


@pytest.fixture
def handled(monkeypatch):
    """Record which circulars were handled, failing the ones told to."""
    seen = []
    failing = set()

    async def handle_record(record, ctx):
        seen.append(record["circularId"])
        if record["circularId"] in failing:
            raise RuntimeError("boom")

    monkeypatch.setattr(main, "handle_record", handle_record)
    monkeypatch.setattr(main, "_seek_back", lambda c, m: c.seek(_Tp(m.offset())))
    return seen, failing


class _Tp:
    def __init__(self, offset):
        self.offset = offset


async def test_a_later_success_does_not_commit_past_a_failure(handled):
    seen, failing = handled
    failing.add(45525)
    consumer = FakeConsumer()
    messages = [FakeMessage(45525, 10), FakeMessage(45527, 11)]

    await main.consume_batch(consumer, messages, {}, {}, 3)

    assert consumer.committed == [], "nothing may be committed past the failure"
    assert consumer.sought == [10], "the failed offset is redelivered"
    assert seen == [45525], "the batch stops at the failure"


async def test_a_success_commits(handled):
    seen, _ = handled
    consumer = FakeConsumer()

    await main.consume_batch(consumer, [FakeMessage(45527, 11)], {}, {}, 3)

    assert consumer.committed == [11]
    assert seen == [45527]


async def test_a_circular_that_keeps_failing_is_given_up_on(handled):
    seen, failing = handled
    failing.add(45525)
    consumer = FakeConsumer()
    failures = {}

    for _ in range(3):
        await main.consume_batch(consumer, [FakeMessage(45525, 10)], {}, failures, 3)

    assert consumer.sought == [10, 10], "retried, then let go"
    assert consumer.committed == [10], "committed only to move past it"
    assert seen == [45525] * 3


async def test_giving_up_does_not_charge_the_next_circular(handled):
    """The attempt count is per offset, not a running total for the partition."""
    seen, failing = handled
    failing.add(45525)
    consumer = FakeConsumer()
    failures = {}

    for _ in range(3):
        await main.consume_batch(consumer, [FakeMessage(45525, 10)], {}, failures, 3)
    failing.clear()
    await main.consume_batch(consumer, [FakeMessage(45528, 11)], {}, failures, 3)

    assert consumer.committed == [10, 11]
    assert failures == {}


async def test_a_failure_clears_the_scoped_session_before_retrying(handled, monkeypatch):
    """A poisoned session would otherwise make every retry fail instantly."""
    seen, failing = handled
    failing.add(45525)
    resets = []
    monkeypatch.setattr(main, "_reset_scoped_session", lambda: resets.append(1))

    consumer = FakeConsumer()
    await main.consume_batch(consumer, [FakeMessage(45525, 10)], {}, {}, 3)

    assert resets, "the session is reset before the offset is sought back"
