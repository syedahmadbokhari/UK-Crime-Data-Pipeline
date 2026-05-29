"""Cache key builders — centralised so key format changes in one place."""


def summary_key(force: str) -> str:
    safe = force.lower().replace(" ", "_").replace("/", "_")
    return f"crimes:summary:{safe}"
