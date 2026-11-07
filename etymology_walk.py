"""Build a full etymology tree by walking the database entry by entry.

An `ety` template records only ONE hop: English gratis was borrowed from Latin
gratis, and that is all. The deeper ancestry lives in the Latin entry's own
`ety` template. Wiktionary renders its tree diagram by following those hops, and
so does this.

    en gratis  --borrowed from-->  la gratis
    la gratis  --contracted from-->  la grātia
    ...

Each hop is a lookup by (term, lang_code), so the walk is only as deep as the
dump has entries for: ancestor forms without their own page stop the branch.

    python etymology_walk.py gratis en
    python etymology_walk.py portmanteau en --depth 6
"""

import argparse
import sys
import unicodedata

import psycopg

import etymology_tree as E

DEFAULT_URI = 'postgresql://postgres:postgres@localhost:5432/wiktionary'


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


def lookup_word(cur, term, lang_code):
    """Find a word id, tolerating diacritics and the reconstruction asterisk."""
    candidates = [term, undiacritic(term)]
    if term.startswith('*'):
        candidates += [term[1:], undiacritic(term[1:])]
    else:
        candidates += ['*' + term, '*' + undiacritic(term)]
    for candidate in dict.fromkeys(candidates):
        cur.execute('SELECT id FROM word WHERE word = %s AND lang_code = %s LIMIT 1',
                    (candidate, lang_code))
        row = cur.fetchone()
        if row:
            return row[0]
    return None


def template_args(cur, template_id):
    cur.execute("""SELECT arg_key, arg_value FROM etymologytemplateargs
                   WHERE etymology_template_id = %s""", (template_id,))
    return dict(cur.fetchall())


def parents_of(cur, term, lang_code):
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
    word_id = lookup_word(cur, term, lang_code)
    if word_id is None:
        return []

    cur.execute("""SELECT id, name FROM etymologytemplate
                   WHERE word_id = %s ORDER BY id""", (word_id,))
    templates = cur.fetchall()

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

    for template_id, name in templates:
        if name in ETYMON_TEMPLATES:
            args = template_args(cur, template_id)
            relation, etymons = split_etymon_args(args)
            for source in etymons:
                node = E.parse(source)
                add(relation, node.get('lang') or args.get('1') or lang_code,
                    node.get('term'), node.get('gloss'), node)
        elif name in PLAIN_TEMPLATES:
            args = template_args(cur, template_id)
            add(E.RELATIONS.get(name, name), args.get('2'), args.get('3'),
                None, None)
    return found


# Cognates are not ancestors: they are related words in sister languages, shown
# in Wiktionary's separate "Cognates" box. Their argument layout differs from
# the ancestry templates -- 1 is the cognate's own language, 2 the term, t the
# gloss -- and the rendered `expansion` is already clean, so it is used as-is.
COGNATE_TEMPLATES = ('cog', 'cog+', 'noncog', 'ncog')


def cognates_of(cur, term, lang_code, langs):
    """[(language, term, gloss, rendered)] for the Cognates box."""
    word_id = lookup_word(cur, term, lang_code)
    if word_id is None:
        return []
    cur.execute("""SELECT id, expansion FROM etymologytemplate
                   WHERE word_id = %s AND name = ANY(%s)
                   ORDER BY id""", (word_id, list(COGNATE_TEMPLATES)))
    out = []
    for template_id, expansion in cur.fetchall():
        args = template_args(cur, template_id)
        code = args.get('1')
        out.append((langs.get(code, code), args.get('2'),
                    args.get('t') or args.get('3'), expansion))
    return out


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


def walk(cur, term, lang_code, depth=0, max_depth=14, seen=None):
    """Yield (depth, relation, lang_code, term, gloss), newest first."""
    seen = seen if seen is not None else set()
    key = (undiacritic(term).lstrip('*'), lang_code)
    if key in seen or depth > max_depth:
        return
    seen.add(key)

    parents = parents_of(cur, term, lang_code)
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
            yield from walk(cur, pterm, plang, depth + 1, max_depth, seen)
        return

    level = depth
    for relation, plang, pterm, gloss, node in parents:
        yield level, relation, plang, pterm, gloss
        if node:
            for sub in node.get('children', []):
                sub_lang = sub.get('lang') or plang
                yield level + 1, sub.get('relation'), sub_lang,                     sub.get('alt') or sub.get('term'), sub.get('gloss')
        deeper = list(walk(cur, pterm, plang, level + 1, max_depth, seen))
        if deeper:
            yield from deeper
            return          # the entry chain took over from here
        level += 1


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('word')
    ap.add_argument('lang_code', nargs='?', default='en')
    ap.add_argument('--uri', default=DEFAULT_URI)
    ap.add_argument('--depth', type=int, default=14)
    ap.add_argument('--cognates', action='store_true',
                    help='also list the Cognates box')
    args = ap.parse_args()

    with psycopg.connect(args.uri, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute('SET statement_timeout=120000')
            langs = E.load_languages(conn)
            name = langs.get(args.lang_code, args.lang_code)
            print(f'{name} {args.word}')

            rows = list(walk(cur, args.word, args.lang_code,
                             max_depth=args.depth))
            if not rows:
                print('   (no etymology tree recorded for this entry)')
                return
            for depth, relation, lang_code, term, gloss in rows:
                lang = langs.get(lang_code, lang_code)
                label = f'{lang} {term}' if term else lang
                if gloss:
                    label += f' (“{gloss}”)'
                arrow = f'--{relation}--> ' if relation else ''
                print('   ' + '  ' * depth + arrow + label)

            if args.cognates:
                rows = cognates_of(cur, args.word, args.lang_code, langs)
                print()
                print(f'Cognates ({len(rows)}):')
                for language, cterm, gloss, expansion in rows:
                    if expansion:
                        print(f'   {expansion}')
                    else:
                        text = f'{language} {cterm}'
                        print(f'   {text}' + (f' (“{gloss}”)' if gloss else ''))


if __name__ == '__main__':
    main()
