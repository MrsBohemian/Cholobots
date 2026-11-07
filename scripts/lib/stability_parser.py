# ABOUTME: Shared parser for monitor-pr-stability.sh output lines.
# ABOUTME: Converts raw stability check results into structured dicts for consumers.


def parse_stability_result(result_line):
    """Parse the output of monitor-pr-stability.sh into a structured dict.

    Possible result lines:
    - STABLE
    - NEEDS_REBASE
    - CHECKS_FAILING:<check1>,<check2>
    - COMMENTS_FOUND:<count>
    - PENDING
    """
    result_line = result_line.strip()

    if result_line == "STABLE":
        return {"status": "stable", "failing_checks": []}

    if result_line == "PENDING":
        return {"status": "pending", "failing_checks": []}

    if result_line == "NEEDS_REBASE":
        return {"status": "needs_rebase", "failing_checks": []}

    if result_line.startswith("CHECKS_FAILING:"):
        raw_checks = result_line.split(":", 1)[1]
        checks = [c.strip() for c in raw_checks.split(",") if c.strip()]
        return {"status": "checks_failing", "failing_checks": checks}

    if result_line.startswith("COMMENTS_FOUND:"):
        count = int(result_line.split(":", 1)[1])
        return {"status": "comments_found", "comment_count": count, "failing_checks": []}

    return {"status": "unknown", "raw": result_line, "failing_checks": []}
