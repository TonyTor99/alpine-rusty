import re
import string

_PUNCT_TRANSLATION = str.maketrans({ch: " " for ch in string.punctuation})
_EXTRA_PUNCT = "«»“”„‟–—−…№"
_EXTRA_TRANSLATION = str.maketrans({ch: " " for ch in _EXTRA_PUNCT})


def normalize(value: str) -> str:
    text = (value or "").lower().replace("ё", "е")
    text = text.translate(_PUNCT_TRANSLATION)
    text = text.translate(_EXTRA_TRANSLATION)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
