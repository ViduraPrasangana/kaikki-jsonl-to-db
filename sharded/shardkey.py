"""Shard routing: which database holds a word.

    key   = fold(word)                    -- NFC, then per-character lowercase
    shard = fnv1a_32(key as UTF-8) % shard_count

Every entry whose word folds to the same key ("Run", "run", "RUN", in every
language) lands on the same shard, so a lookup is one query on one database.

Any backend that reads the shards must compute exactly the same thing. Two
details make that portable to other languages:

  * Lowercasing is per character (Unicode simple lowercase mapping), never on
    the whole string. Python's str.lower() on a string applies the final-sigma
    rule ("ΟΔΟΣ" -> "οδος" with a final ς); most other languages' per-character
    lowercase functions do not.
  * U+0130 (İ) is the only character whose Python lowercase is two characters
    ("i̇"). Its simple mapping is plain "i", so we use that.

Do not swap the hash for a language's built-in string hash (Python's hash(),
Go's map hash, ...): those are randomised per process.
"""

import unicodedata

FNV_OFFSET = 0x811C9DC5
FNV_PRIME = 0x01000193

# Characters whose str.lower() is not a single character -> Unicode simple
# lowercase mapping. Unicode 15.0 has exactly one.
_SIMPLE_LOWER = {'İ': 'i'}


def fold(word):
    """Normalised lookup key for a word."""
    out = []
    for ch in unicodedata.normalize('NFC', word):
        lower = ch.lower()
        out.append(lower if len(lower) == 1 else _SIMPLE_LOWER.get(ch, ch))
    return ''.join(out)


def fnv1a_32(data):
    h = FNV_OFFSET
    for byte in data:
        h ^= byte
        h = (h * FNV_PRIME) & 0xFFFFFFFF
    return h


def shard_for(word, shard_count):
    return fnv1a_32(fold(word).encode('utf-8')) % shard_count


if __name__ == '__main__':
    import sys
    count = int(sys.argv[1])
    for w in sys.argv[2:]:
        print(f'{w!r:24} key={fold(w)!r:24} shard={shard_for(w, count)}')
