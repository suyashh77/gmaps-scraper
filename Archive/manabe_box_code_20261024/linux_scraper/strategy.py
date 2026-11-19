"""
Per-store scraping decisions, kept apart from the browser code so they can be tested.

Sort ladder
    Every store starts on Newest. When the panel stops loading reviews for
    STALL_SWITCH scroll cycles, the scraper moves to the next rung instead of stopping:
    Newest -> Most relevant (Google's default) -> Lowest rating -> Highest rating.
    Google Maps offers exactly these four orderings; there is no "oldest", so the two
    rating orders are the "earliest"-style fallbacks: each surfaces a different slice of
    the listing, including older reviews the time-ordered views do not reach.

Completion
    A store is complete when we hold its target, or when we hold at least as many reviews
    as Google's live count on a healthy page. The second rule is the one that was missing:
    a store with a stale target one above its live count was requeued forever.
"""
from __future__ import annotations

SORT_LADDER = ["newest", "relevant", "lowest", "highest"]
STALL_SWITCH = 60          # scroll cycles with nothing new -> next rung

COMPLETED, PROGRESSED, FAILED = "completed", "progressed", "failed"

# "Good enough" (research lead, 24 Sep 2026): a store with more than 100 reviews is done
# within 25 of its target; one with 100 or fewer within 5. A store with any reviews at all
# still needs at least one.
BIG_STORE = 100
BIG_STORE_SLACK = 25
SMALL_STORE_SLACK = 5


def done_at(target: int) -> int:
    """Reviews we must hold for a store with this target to count as complete."""
    if target <= 0:
        return 0
    slack = BIG_STORE_SLACK if target > BIG_STORE else SMALL_STORE_SLACK
    return max(target - slack, 1)


def sort_ladder(resume_from: str | None = None) -> list[str]:
    """The rungs to try, in order. An interrupted run resumes on the rung it was on."""
    if resume_from in SORT_LADDER:
        return SORT_LADDER[SORT_LADDER.index(resume_from):]
    return list(SORT_LADDER)


def should_switch_sort(stall_cycles: int) -> bool:
    return stall_cycles >= STALL_SWITCH


def _live_trusted(live: int, limited_view: bool) -> bool:
    return live > 0 and not limited_view


def nothing_to_collect(held: int, live: int, limited_view: bool) -> bool:
    """We already hold (within tolerance) every review a healthy page says exists."""
    return _live_trusted(live, limited_view) and held >= done_at(live)


def outcome(held: int, target: int, live: int, limited_view: bool, gained: int) -> str:
    """How a pass ends. held = master + batch reviews for the store; gained = new this pass."""
    if held >= done_at(target) or nothing_to_collect(held, live, limited_view):
        return COMPLETED
    return PROGRESSED if gained > 0 else FAILED
