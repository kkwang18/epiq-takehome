# tests/test_stub_state.py
from content_intake.stub.state import StubState


def make_state(**overrides):
    defaults = dict(
        latency_mode="fixed", latency_ms=0, latency_jitter_min_ms=50, latency_jitter_max_ms=250,
        latency_seed=1, failure_every_n=0, failure_status=500, in_flight_capacity=2,
    )
    defaults.update(overrides)
    return StubState(**defaults)


def test_capacity_gate_admits_up_to_capacity():
    s = make_state(in_flight_capacity=2)
    assert s.enter_call() is True
    assert s.enter_call() is True
    assert s.enter_call() is False
    s.exit_call()
    assert s.enter_call() is True


def test_failure_schedule_fires_every_nth_billed_call():
    s = make_state(failure_every_n=3)
    results = []
    for _ in range(9):
        s.bill()
        results.append(s.should_fail_this_billed_call())
    assert results == [False, False, True, False, False, True, False, False, True]


def test_failure_schedule_disabled_when_zero():
    s = make_state(failure_every_n=0)
    for _ in range(20):
        s.bill()
        assert s.should_fail_this_billed_call() is False


def test_annotation_stable_for_same_content():
    s = make_state()
    a1 = s.annotate(b"hello")
    a2 = s.annotate(b"hello")
    assert a1["sha256"] == a2["sha256"]
    assert a1["result"] == a2["result"]


def test_annotation_includes_sha256_of_content():
    import hashlib
    s = make_state()
    a = s.annotate(b"hello")
    assert a["sha256"] == hashlib.sha256(b"hello").hexdigest()


def test_fixed_latency_returns_configured_value():
    s = make_state(latency_mode="fixed", latency_ms=150)
    assert s.next_latency_ms() == 150


def test_jitter_latency_within_bounds_and_repeatable_with_same_seed():
    s1 = make_state(latency_mode="jitter", latency_seed=42, latency_jitter_min_ms=50, latency_jitter_max_ms=250)
    s2 = make_state(latency_mode="jitter", latency_seed=42, latency_jitter_min_ms=50, latency_jitter_max_ms=250)
    seq1 = [s1.next_latency_ms() for _ in range(10)]
    seq2 = [s2.next_latency_ms() for _ in range(10)]
    assert seq1 == seq2
    assert all(50 <= v <= 250 for v in seq1)


def test_stats_reports_required_fields():
    s = make_state(in_flight_capacity=2)
    s.bill(); s.enter_call()
    s.record_server_error()
    s.record_over_capacity()
    stats = s.stats()
    for key in ("billed_calls", "current_in_flight", "max_in_flight", "server_error_calls", "over_capacity_calls"):
        assert key in stats


def test_max_in_flight_tracks_observed_peak_not_configured_cap():
    s = make_state(in_flight_capacity=5)
    s.enter_call(); s.enter_call()
    assert s.stats()["max_in_flight"] == 2
    s.exit_call(); s.exit_call()
    assert s.stats()["max_in_flight"] == 2


def test_reset_clears_counters_and_restarts_sequences():
    s = make_state(failure_every_n=2)
    s.bill(); s.should_fail_this_billed_call()
    s.bill(); s.should_fail_this_billed_call()
    s.reset(None)
    assert s.stats()["billed_calls"] == 0
    s.bill()
    assert s.should_fail_this_billed_call() is False
    s.bill()
    assert s.should_fail_this_billed_call() is True


def test_reset_applies_partial_config_patch():
    s = make_state(failure_every_n=7)
    s.reset({"failure_every_n": 1})
    s.bill()
    assert s.should_fail_this_billed_call() is True


def test_bill_and_check_failure_matches_sequential_bill_and_check():
    s = make_state(failure_every_n=3)
    results = []
    for _ in range(9):
        results.append(s.bill_and_check_failure())
    assert results == [False, False, True, False, False, True, False, False, True]


def test_bill_and_check_failure_exact_count_under_concurrency():
    # The bug this guards against: bill() + should_fail_this_billed_call() as two
    # separately-locked calls can let concurrent threads interleave between them,
    # corrupting the failure COUNT (not just which item fails). bill_and_check_failure()
    # must not have this race: with failure_every_n=2 and 20 concurrent calls, exactly
    # 10 must be flagged as failures, every time, regardless of thread interleaving.
    import threading

    s = make_state(failure_every_n=2)
    results = []
    lock = threading.Lock()

    def call():
        result = s.bill_and_check_failure()
        with lock:
            results.append(result)

    threads = [threading.Thread(target=call) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 20
    assert sum(1 for r in results if r) == 10
