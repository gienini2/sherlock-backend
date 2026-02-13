# resolver/similarity.py

def plate_similarity(a: str, b: str) -> float:
    matches = sum(1 for x, y in zip(a, b) if x == y)
    return matches / max(len(a), len(b))