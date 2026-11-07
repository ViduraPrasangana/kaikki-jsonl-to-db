"""Parser for Wiktionary etymology-tree etymon expressions.

kaikki stores the `ety` template's third argument verbatim, e.g.

    la:gratis
    frm:portemanteau<ety:af<porter<alt:porte><t:he carries>><manteau<t:coat>>>

That string is the etymology tree. wiktextract does not expand it, and the
`etymology_text` rendered alongside it is mangled (relation markers get glued
onto words: "Latin gratisbor."), so this module parses the source instead.

Grammar, derived from Template:etymon and verified against the dump:

    etymon    := [langcode ':'] term modifier*
    modifier  := '<' key ':' value '>'
    key       := 't' | 'id' | 'alt' | 'ref' | 'pos' | 'name' | 'g' | 'lit'
               | 'tr' | 'sc' | 'q'          -- scalar annotations
               | 'ety'                      -- inline etymology, see below
               | <langcode>                 -- a child etymon
    ety value := relation modifier*         -- e.g. af<porter><manteau>

`>` outside a modifier separates a descent chain (enm>fader>father), oldest
first.

Usage:

    from etymology_tree import parse, render, flatten
    tree = parse('la:gratis')
    render(tree)                      -> 'Latin gratis'
    render(parse(nested), relation='bor')

    python etymology_tree.py 'frm:portemanteau<t:coat>'
"""

import re
import sys

# Relation codes -> readable phrasing. From Wiktionary's etymology templates.
RELATIONS = {
    'inh': 'inherited from', 'inh+': 'Inherited from',
    'bor': 'borrowed from', 'bor+': 'Borrowed from',
    'der': 'derived from', 'der+': 'Derived from',
    'ubor': 'unadapted borrowing from',
    'lbor': 'learned borrowing from',
    'slbor': 'semi-learned borrowing from',
    'obor': 'orthographic borrowing from',
    'cal': 'calque of', 'clq': 'calque of',
    'sl': 'semantic loan from',
    'af': 'affix of', 'affix': 'affix of',
    'suf': 'suffix of', 'suffix': 'suffix of',
    'pre': 'prefix of', 'prefix': 'prefix of',
    'com': 'compound of', 'compound': 'compound of',
    'blend': 'blend of',
    'clipping': 'clipping of',
    'back-form': 'back-formation from',
    'cog': 'cognate with',
    'root': 'from the root',
    'doublet': 'doublet of',
    'abbrev': 'abbreviation of',
    'onomatopoeic': 'onomatopoeic',
    'unk': 'of unknown origin',
}

# Scalar annotations; anything else before ':' inside <> is a language code.
SCALAR_KEYS = {'t', 'id', 'alt', 'ref', 'pos', 'name', 'g', 'lit',
               'tr', 'sc', 'q', 'lit', 'gloss'}

_LANG_RE = re.compile(r'^[a-z]{2,3}(-[a-z0-9]+)*$', re.IGNORECASE)


class EtymologyParseError(ValueError):
    pass


def _split_groups(text, strict=True):
    """Split trailing <...> groups off a head, respecting nesting.

    'porter<alt:porte><t:he carries>' -> ('porter', ['alt:porte', 't:he carries'])

    A small number of entries in the dump are genuinely malformed (unbalanced
    brackets written by hand on Wiktionary, or a <ref:...> containing raw HTML).
    With strict=False an unclosed group is closed at end of input rather than
    discarding the whole expression.
    """
    head_end = None
    groups = []
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == '<':
            if depth == 0:
                if head_end is None:
                    head_end = i
                start = i + 1
            depth += 1
        elif ch == '>':
            depth -= 1
            if depth == 0:
                groups.append(text[start:i])
            elif depth < 0:
                if strict:
                    raise EtymologyParseError(f'unbalanced > in {text!r}')
                depth = 0          # stray '>', treat as literal
    if depth != 0:
        if strict:
            raise EtymologyParseError(f'unbalanced < in {text!r}')
        if start is not None:
            groups.append(text[start:])   # close the dangling group
    head = text if head_end is None else text[:head_end]
    return head, groups


def _split_key(group):
    """Split 'key:value' at the first colon outside any nested group."""
    depth = 0
    for i, ch in enumerate(group):
        if ch == '<':
            depth += 1
        elif ch == '>':
            depth -= 1
        elif ch == ':' and depth == 0:
            return group[:i], group[i + 1:]
    return None, group


def parse(value, strict=False):
    """Parse an etymon expression into a nested dict.

    Returns {lang, term, gloss, alt, id, ref, pos, relation, children, chain}.
    strict=True raises on malformed input; the default recovers instead, which
    matters because ~0.01% of the dump is malformed at source.
    """
    if value is None:
        raise EtymologyParseError('value is None')
    value = value.strip()
    if not value:
        raise EtymologyParseError('empty value')

    # A top-level '>' chain is a descent sequence, oldest first.
    chain = _split_chain(value)
    if len(chain) > 1:
        nodes = [parse(part, strict) for part in chain]
        root = nodes[0]
        root['chain'] = nodes[1:]
        return root

    head, groups = _split_groups(value, strict)

    lang, term = _split_key(head)
    if lang is not None and not _LANG_RE.match(lang):
        # Not a language code after all; the colon belonged to the term.
        lang, term = None, head

    node = {'lang': lang, 'term': term.strip() or None, 'gloss': None,
            'alt': None, 'id': None, 'ref': None, 'pos': None,
            'relation': None, 'children': [], 'chain': []}

    for group in groups:
        key, val = _split_key(group)
        if key is None:
            node['children'].append(parse(group, strict))
        elif key == 'ety':
            relation, sub = _split_groups(val, strict)
            node['relation'] = relation.strip() or None
            node['children'].extend(parse(s, strict) for s in sub)
        elif key in SCALAR_KEYS:
            field = 'gloss' if key in ('t', 'gloss') else key
            node[field] = val
        elif _LANG_RE.match(key):
            node['children'].append(parse(group, strict))
        else:
            # Unknown key: keep it rather than silently dropping data.
            node.setdefault('extra', {})[key] = val
    return node


def _split_chain(text):
    """Split on '>' that is not part of a <...> group."""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(text):
        if ch == '<':
            depth += 1
        elif ch == '>':
            if depth == 0:
                parts.append(text[start:i])
                start = i + 1
            else:
                depth -= 1
    parts.append(text[start:])
    return [p for p in parts if p.strip()]


def render(node, relation=None, langs=None):
    """Render one node as readable text: 'Latin gratis ("thanks")'."""
    if node is None:
        return ''
    lang = node.get('lang')
    name = (langs or {}).get(lang, lang) if lang else None
    term = node.get('alt') or node.get('term') or ''
    out = f'{name} {term}'.strip() if name else term
    if node.get('gloss'):
        out += f' (“{node["gloss"]}”)'
    if relation:
        out = f'{RELATIONS.get(relation, relation)} {out}'
    return out


def flatten(node, depth=0):
    """Depth-first list of (depth, relation, node) for tree display."""
    rows = [(depth, node.get('relation'), node)]
    for child in node.get('children', []):
        rows.extend(flatten(child, depth + 1))
    for nxt in node.get('chain', []):
        rows.extend(flatten(nxt, depth))
    return rows


def describe(node, langs=None):
    """One-line summary: 'Latin gratis, affix of porte + manteau'."""
    parts = [render(node, langs=langs)]
    if node.get('relation') and node['children']:
        joined = ' + '.join(render(c, langs=langs) for c in node['children'])
        parts.append(f'{RELATIONS.get(node["relation"], node["relation"])} {joined}')
    # A '>' chain is a descent sequence; show it oldest -> newest.
    if node.get('chain'):
        steps = [render(node, langs=langs)] + [render(n, langs=langs) for n in node['chain']]
        return ' > '.join(s for s in steps if s)
    return ', '.join(p for p in parts if p)


# Ancestor languages that appear in etymologies but have no entries of their
# own in the dump, so no name can be read from the Word table.
EXTRA_LANGUAGES = {
    'itc-ola': 'Old Latin', 'itc-pro': 'Proto-Italic',
    'ine-pro': 'Proto-Indo-European', 'gem-pro': 'Proto-Germanic',
    'gmw-pro': 'Proto-West Germanic', 'sla-pro': 'Proto-Slavic',
    'bat-pro': 'Proto-Baltic', 'cel-pro': 'Proto-Celtic',
    'iir-pro': 'Proto-Indo-Iranian', 'grk-pro': 'Proto-Hellenic',
    'la-med': 'Medieval Latin', 'la-lat': 'Late Latin',
    'la-new': 'New Latin', 'la-vul': 'Vulgar Latin',
    'enm': 'Middle English', 'ang': 'Old English',
    'frm': 'Middle French', 'fro': 'Old French',
    'goh': 'Old High German', 'dum': 'Middle Dutch',
}


def load_languages(conn):
    """lang_code -> language name, from the dump's own Word rows.

    Proto-languages are only ever cited, never given entries, so the Word table
    alone leaves them as bare codes; EXTRA_LANGUAGES fills those in.
    """
    languages = dict(EXTRA_LANGUAGES)
    with conn.cursor() as cur:
        cur.execute('SELECT DISTINCT lang_code, lang FROM Word WHERE lang IS NOT NULL')
        languages.update({code: name for code, name in cur.fetchall() if code})
    return languages


if __name__ == '__main__':
    for arg in sys.argv[1:]:
        tree = parse(arg)
        print(f'input : {arg}')
        print(f'summary: {describe(tree)}')
        for depth, relation, node in flatten(tree):
            marker = f'[{relation}] ' if relation else ''
            print('  ' * (depth + 1) + marker + render(node))
        print()
