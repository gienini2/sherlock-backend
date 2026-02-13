# annotator/text_marker.py

def mark_text(text: str, resolutions: list) -> str:
    for r in resolutions:
        if r["match"] == "exact":
            text = text.replace(r["entity"], f"@{r['entity']}")
        elif r["match"] == "partial":
            text = text.replace(r["entity"], f"**{r['entity']}**")
    return text