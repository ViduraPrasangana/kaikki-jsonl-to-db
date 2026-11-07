"""Build a full etymology tree from the sharded database.

Same tree, same output as ../etymology_walk.py (which reads the 41 GB
relational database), but each entry comes from its shard as one compressed
JSON row, and lookups are batched:

    ../etymology_walk.py   per step: 1-4 word lookups + 1 template query +
                           1 query per template    -> 40-60 sequential queries
    this script            per tree LEVEL: one query per shard, run in
                           parallel                -> ~1-3 queries per level

How: an entry's parents depend only on that entry's own JSON, so the tree is
first fetched breadth-first, a whole level per round, into a cache. The
original depth-first walk then runs unchanged against the cache, so ordering,
de-duplication and depth limits behave exactly as before.

One deliberate difference: a word with several etymologies (homographs, e.g.
robot: German Robot / Czech robot / the 4chan sense) gets one tree per
etymology, like Wiktionary's "Etymology 1, 2, 3". The relational script
started from whichever of those rows PostgreSQL happened to return first.
Ancestors further up are resolved as before: first entry of that spelling.

    # shard URLs come from shard_urls.txt (or SHARD_URLS, or --shard-url)
    python etymology_walk.py gratis en
    python etymology_walk.py portmanteau en --depth 6 --cognates --stats
"""

import argparse
import json
import os
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import etymology_tree as E                                  # noqa: E402  (parser lives one folder up)

import shardkey                                             # noqa: E402
from shardclient import Shards, default_urls               # noqa: E402


def undiacritic(text):
    """grātia -> gratia. Wiktionary page titles carry no length marks, but the
    etymon arguments cite the accented scholarly form, so a lookup on the exact
    string misses the entry and the tree stops one hop short."""
    return ''.join(ch for ch in unicodedata.normalize('NFD', text)
                   if unicodedata.category(ch) != 'Mn')


# Templates that record "this word came from that word".
# 'ety' / 'etymon' use   1=this lang, 2=:relation, 3,4,..=parents (etymon syntax)
# the rest use           1=this lang, 2=source lang code, 3=source term
ETYMON_TEMPLATES = ('ety', 'etymon')
PLAIN_TEMPLATES = ('inh', 'bor', 'der', 'lbor', 'ubor', 'obor', 'slbor',
                   'af', 'affix', 'suf', 'suffix', 'pre', 'prefix',
                   'com', 'compound', 'cal', 'clq', 'blend')

# Cognates are not ancestors: they are related words in sister languages, shown
# in Wiktionary's separate "Cognates" box. Their argument layout differs from
# the ancestry templates -- 1 is the cognate's own language, 2 the term, t the
# gloss -- and the rendered `expansion` is already clean, so it is used as-is.
COGNATE_TEMPLATES = ('cog', 'cog+', 'noncog', 'ncog')


def candidates(term):
    """Spellings to try, in priority order: tolerate diacritics and the
    reconstruction asterisk."""
    out = [term, undiacritic(term)]
    if term.startswith('*'):
        out += [term[1:], undiacritic(term[1:])]
    else:
        out += ['*' + term, '*' + undiacritic(term)]
    return list(dict.fromkeys(out))


class EntryCache:
    """(term, lang_code) -> the entry's JSON, or None. Fetches in batches."""

    SQL = """SELECT id, word, lang_code, data FROM entry
             WHERE word_key = ANY(%s) AND lang_code = ANY(%s)
             ORDER BY id"""

    def __init__(self, shards):
        self.shards = shards
        self.entries = {}
        self.matches = {}             # (term, lang) -> compressed rows of the matched spelling, id order
        self.queries = 0
        self.rounds = 0
        self.languages = {}           # lang_code -> name, learned from fetched entries

    def prefetch(self, pairs):
        """Resolve many (term, lang_code) pairs in one round: one query per shard
        involved, all shards in parallel."""
        pairs = [p for p in dict.fromkeys(pairs) if p not in self.entries]
        if not pairs:
            return
        n = len(self.shards.conns)
        wanted = [(set(), set()) for _ in range(n)]          # per shard: keys, lang codes
        for term, lang in pairs:
            for candidate in candidates(term):
                key = shardkey.fold(candidate)
                keys, langs = wanted[shardkey.fnv1a_32(key.encode('utf-8')) % n]
                keys.add(key)
                langs.add(lang)

        def query(i):
            keys, langs = wanted[i]
            return self.shards.conns[i].execute(self.SQL, (list(keys), list(langs))).fetchall()

        busy = [i for i in range(n) if wanted[i][0]]
        with ThreadPoolExecutor(len(busy)) as ex:
            results = list(ex.map(query, busy))
        self.queries += len(busy)
        self.rounds += 1

        # Group by exact spelling: word_key is case-folded, the original
        # match (word = %s) was not. Each spelling lives on one shard and rows
        # arrive ORDER BY id, so each list is in id (= dump) order.
        by_spelling = {}
        for rows in results:
            for _row_id, word, lang, data in rows:
                by_spelling.setdefault((word, lang), []).append(data)

        # A term's entry is the first row of the first candidate spelling
        # that exists.
        for term, lang in pairs:
            entry, rows = None, []
            for candidate in candidates(term):
                rows = by_spelling.get((candidate, lang), [])
                if rows:
                    entry = self.shards.decode(rows[0])
                    if entry.get('lang') and lang not in self.languages:
                        self.languages[lang] = entry['lang']
                    break
            self.entries[(term, lang)] = entry
            self.matches[(term, lang)] = rows

    def get(self, term, lang):
        if (term, lang) not in self.entries:
            self.prefetch([(term, lang)])                    # cache miss: still correct, one more round
        return self.entries[(term, lang)]

    def etymologies(self, term, lang):
        """One entry per distinct etymology of a word, in dump order. Entries
        of one etymology section (noun + verb under "Etymology 1") share the
        etymology number and templates, so they collapse to the first."""
        self.get(term, lang)
        out, seen = [], set()
        for data in self.matches.get((term, lang), []):
            entry = self.shards.decode(data)
            key = (entry.get('etymology_number'),
                   json.dumps(entry.get('etymology_templates'), sort_keys=True))
            if key not in seen:
                seen.add(key)
                out.append(entry)
        return out


def templates(entry):
    return (entry.get('etymology_templates') or []) if entry else []


def parents_of(cache, term, lang_code):
    """Immediate parents of a word, plus the deeper chain the entry records.

    An entry often names several ancestors at once, nearest first:

        la:-ius   der itc-ola -ios     <- immediate parent
                  inh itc-pro *-ios    <- its parent
                  inh ine-pro *-yos    <- and its parent

    Wiktionary's tree uses all of them, which matters because intermediate
    languages such as Old Latin have no entries of their own in the dump, so
    following entry-to-entry links alone stops the branch early.

    Returns [(relation, lang, term, gloss, parsed_node)] ordered nearest first;
    the caller nests them as a chain.
    """
    entry = cache.get(term, lang_code)
    if entry is None:
        return []

    found = []
    seen = set()

    def add(relation, lang, tterm, gloss, node):
        # '-' and bare punctuation turn up where a template argument was left
        # empty; they are not words.
        if not tterm or not lang or not tterm.strip('-*: '):
            return
        key = (lang, undiacritic(tterm).lstrip('*'))
        if key in seen:
            return
        seen.add(key)
        found.append((relation, lang, tterm, gloss, node))

    for template in templates(entry):
        name = template.get('name')
        args = template.get('args') or {}
        if name in ETYMON_TEMPLATES:
            relation, etymons = split_etymon_args(args)
            for source in etymons:
                node = E.parse(source)
                add(relation, node.get('lang') or args.get('1') or lang_code,
                    node.get('term'), node.get('gloss'), node)
        elif name in PLAIN_TEMPLATES:
            add(E.RELATIONS.get(name, name), args.get('2'), args.get('3'),
                None, None)
    return found


def split_etymon_args(args):
    """(relation, [etymon strings]) from etymon-syntax arguments.

    Argument 2 is the relation when it starts with ':', otherwise it is already
    the first parent. Arguments 3, 4, 5... are further parents: Latin gratia is
    3='grātus' 4='-ia', so reading only argument 3 loses half the tree.
    """
    relation = None
    etymons = []
    for key in sorted((k for k in args if k.isdigit() and int(k) >= 2), key=int):
        value = args[key]
        if key == '2' and value.startswith(':'):
            head, groups = E._split_groups(value.lstrip(':'), strict=False)
            relation = E.RELATIONS.get(head, head)
            for group in groups:
                gkey, gval = E._split_key(group)
                if gkey == 'text':
                    relation = gval
        elif value and not value.startswith(':'):
            # A ':xxx' in a later slot is another relation marker, not a word.
            etymons.append(value)
    return relation, etymons


def prefetch_tree(cache, term, lang_code, max_depth):
    """Fetch every entry the walk can reach, one tree level per round.

    Fetches a superset of what walk() visits: walk() can stop a branch early
    (seen set, depth, 'the entry chain took over'), but it never needs an
    entry this loop has not reached within max_depth levels. Anything missed
    would still be fetched on demand by EntryCache.get.
    """
    frontier = [(term, lang_code)]
    done = set()
    for _depth in range(max_depth + 1):
        cache.prefetch(frontier)
        done.update(frontier)
        nxt = []
        for t, l in frontier:
            for _rel, plang, pterm, _gloss, _node in parents_of(cache, t, l):
                if (pterm, plang) not in done:
                    nxt.append((pterm, plang))
        frontier = list(dict.fromkeys(nxt))
        if not frontier:
            break


def walk(cache, term, lang_code, depth=0, max_depth=14, seen=None):
    """Yield (depth, relation, lang_code, term, gloss), newest first."""
    seen = seen if seen is not None else set()
    key = (undiacritic(term).lstrip('*'), lang_code)
    if key in seen or depth > max_depth:
        return
    seen.add(key)

    parents = parents_of(cache, term, lang_code)
    if not parents:
        return

    # Several parents from ONE etymon template are components of a compound
    # (grātus + -ia) and are siblings. Ancestors gathered from separate
    # templates are successive generations, so they nest.
    first_relation = parents[0][0]
    siblings = [p for p in parents if p[0] == first_relation and p[4] is not None]
    if len(siblings) > 1:
        for relation, plang, pterm, gloss, node in siblings:
            yield depth, relation, plang, pterm, gloss
            yield from walk(cache, pterm, plang, depth + 1, max_depth, seen)
        return

    level = depth
    for relation, plang, pterm, gloss, node in parents:
        yield level, relation, plang, pterm, gloss
        if node:
            for sub in node.get('children', []):
                sub_lang = sub.get('lang') or plang
                yield level + 1, sub.get('relation'), sub_lang, \
                    sub.get('alt') or sub.get('term'), sub.get('gloss')
        deeper = list(walk(cache, pterm, plang, level + 1, max_depth, seen))
        if deeper:
            yield from deeper
            return          # the entry chain took over from here
        level += 1


def cognates_of(cache, term, lang_code, langs):
    """[(language, term, gloss, rendered)] for the Cognates box."""
    out = []
    for template in templates(cache.get(term, lang_code)):
        if template.get('name') in COGNATE_TEMPLATES:
            args = template.get('args') or {}
            code = args.get('1')
            out.append((langs.get(code, code), args.get('2'),
                        args.get('t') or args.get('3'), template.get('expansion')))
    return out


def load_languages(shards):
    """lang_code -> name. Uses the shards' `language` table when present
    (see add_languages.py); names of fetched entries fill in the rest."""
    languages = dict(E.EXTRA_LANGUAGES)
    try:
        rows = shards.conns[0].execute('SELECT lang_code, name FROM language').fetchall()
        languages.update(rows)
    except Exception:                                        # noqa: BLE001  table not built yet
        pass
    return languages


def trees(shards, word, lang_code, max_depth=14, langs=None):
    """([(root entry, tree rows, cognates)], cache): one tree per etymology.

    For each etymology: point the cache's root at that entry, prefetch level by
    level (entries fetched for an earlier tree are reused), then walk.
    """
    cache = EntryCache(shards)
    roots = cache.etymologies(word, lang_code) or [None]
    out = []
    for root in roots:
        cache.entries[(word, lang_code)] = root
        prefetch_tree(cache, word, lang_code, max_depth)
        rows = list(walk(cache, word, lang_code, max_depth=max_depth))
        out.append((root, rows, cognates_of(cache, word, lang_code, langs or {})))
    if langs is not None:
        for code, name in cache.languages.items():
            langs.setdefault(code, name)
    return out, cache


def print_tree(word, lang_code, rows, langs, label=''):
    name = langs.get(lang_code, lang_code)
    print(f'{name} {word}{label}')
    if not rows:
        print('   (no etymology tree recorded for this entry)')
        return
    for depth, relation, lc, term, gloss in rows:
        lang = langs.get(lc, lc)
        label = f'{lang} {term}' if term else lang
        if gloss:
            label += f' (“{gloss}”)'
        arrow = f'--{relation}--> ' if relation else ''
        print('   ' + '  ' * depth + arrow + label)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('word')
    ap.add_argument('lang_code', nargs='?', default='en')
    ap.add_argument('--shard-url', action='append', default=[])
    ap.add_argument('--depth', type=int, default=14)
    ap.add_argument('--cognates', action='store_true',
                    help='also list the Cognates box')
    ap.add_argument('--stats', action='store_true', help='print query count and time')
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding='utf-8')   # IPA and reconstructions on a Windows console

    with Shards(args.shard_url or default_urls()) as shards:
        langs = load_languages(shards)
        started = time.perf_counter()
        results, cache = trees(shards, args.word, args.lang_code, args.depth, langs)
        elapsed = time.perf_counter() - started

        for i, (root, rows, cognates) in enumerate(results):
            if i:
                print()
            label = ''
            if len(results) > 1:
                label = f' (etymology {(root or {}).get("etymology_number") or i + 1})'
            print_tree(args.word, args.lang_code, rows, langs, label)

            if args.cognates and rows:
                print()
                print(f'Cognates ({len(cognates)}):')
                for language, cterm, gloss, expansion in cognates:
                    if expansion:
                        print(f'   {expansion}')
                    else:
                        text = f'{language} {cterm}'
                        print(f'   {text}' + (f' (“{gloss}”)' if gloss else ''))

        if args.stats:
            found = sum(1 for e in cache.entries.values() if e is not None)
            print(f'\n-- {cache.queries} queries in {cache.rounds} rounds, '
                  f'{found}/{len(cache.entries)} entries found, {elapsed * 1000:.0f} ms')


if __name__ == '__main__':
    main()
