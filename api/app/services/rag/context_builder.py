"""Formats retrieved crime data into a plain-text context string for Gemini."""


def build_context(data: dict) -> str:
    """Convert retrieved crime statistics into a readable context block."""
    if data["total"] == 0:
        return "No crime data found matching the query."

    months = data.get("months", [])
    period = f"{months[0]} to {months[-1]}" if len(months) > 1 else (months[0] if months else "unknown")

    lines = [
        f"Force area: {data['force']}",
        f"Period: {period}",
        f"Total crimes: {data['total']:,}",
        "",
        "Crime type breakdown:",
    ]

    for item in data["type_distribution"]:
        lines.append(f"  - {item['crime_type']}: {item['count']:,} crimes ({item['pct']}%)")

    if data.get("outcome_distribution"):
        lines.append("")
        lines.append("Outcome breakdown:")
        for item in data["outcome_distribution"]:
            lines.append(f"  - {item['outcome']}: {item['count']:,}")

    return "\n".join(lines)
