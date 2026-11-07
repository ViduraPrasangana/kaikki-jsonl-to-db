# Etymology links: finding them and resolving them

Reference for building an etymology API that returns **the etymology text, the
text of each link, and where each link points**. It covers:

- which kaikki / wiktextract fields hold etymology links,
- how to find each link's text inside `etymology_text`,
- how to turn a link into a Wiktionary URL and into a lookup in our own data,
- what is possible with the older dump we have locally.

Everything marked *measured* was checked against real data (the local dump
from 2026-08-17 and current kaikki.org JSON fetched on 2026-10-27). Rules marked
*Wiktionary docs* come from Wiktionary's template and module documentation
(`Template:link`, `Template:affix`, `Template:etymon`, `Template:senseid`,
`Module:languages`).

---

## 0. The short answer

| Dump | Has `etymology_links`? | What to do |
|---|---|---|
| wiktextract **after 2026-09-21**, including our local `raw-wiktextract-data.jsonl` downloaded **2026-10-25** (26.6 GB) | **Yes** | Use `etymology_links`. Wiktionary resolved every target itself. Locate each link's text in `etymology_text` (section 3). Turn targets into URLs and lookups (section 4). |
| Older dumps, e.g. the one we had before (2026-08-17) | **No** | Only template arguments are available, and about a third of prose links are missing entirely (section 6). |

*Measured* on the 2026-10-25 dump (random sample of 30,000 lines):
- 94.1% of entries with `etymology_text` have `etymology_links`. The rest are
  mostly link-less texts: "See the etymology of the corresponding lemma form"
  (S13), "Unknown.", "Onomatopoeic.", "From Latin.".
- 55,172 links; **99.71%** placed in the text with the section 3 algorithm.

`etymology_links` arrived in wiktextract **PR #1709** ("Preserve etymology links
across supported Wiktionary editions", merged **2026-09-21**). Our dump predates
it by five weeks. The sharded build stores each JSON line whole, so a rebuild
from a new dump gets the field with **no schema or code changes**.

---

## 1. Where links come from on a Wiktionary etymology

Every clickable word in a Wiktionary etymology comes from one of these sources:

| # | Source in wikitext | Example | In `etymology_templates`? | In `etymology_links` (new dumps)? |
|---|---|---|---|---|
| L1 | Ancestry templates `{{inh}}`, `{{bor}}`, `{{der}}`, `{{lbor}}`, `{{cal}}`, … | `{{inh\|en\|enm\|fader}}` | yes | yes |
| L2 | Word-formation templates `{{af}}`, `{{suffix}}`, `{{compound}}`, `{{blend}}`, … | `{{suffix\|en\|lie\|ar}}` | yes | yes |
| L3 | Etymon templates `{{ety}}` / `{{etymon}}`: text (`text=+`) **and** the rendered tree (`tree=1`) | `{{ety\|en\|:af\|fade<pos:verb>\|-er<id:agent noun>\|tree=1}}` | yes, but only one step of the tree is in `args` | yes, including **every tree node** |
| L4 | Relatives `{{cog}}`, `{{ncog}}`, `{{noncog}}`, `{{doublet}}`, `{{m+}}`, … | `{{cog\|de\|Vater}}` | yes | yes |
| L5 | **Mention and link templates** `{{m}}`, `{{mention}}`, `{{l}}`, `{{link}}`, `{{ll}}`, `{{m-self}}`, `{{l-self}}` | `the adjective {{m\|la\|salārius\|\|related to salt}}` | **no, wiktextract drops them** | yes |
| L6 | **Plain wikilinks** `[[word]]`, `[[target\|shown]]` | `under the influence of [[question]]` | **no** | yes, often without a language anchor |
| L7 | Language-specific link templates `{{ja-r}}`, `{{zh-l}}`, `{{ko-l}}`, `{{vi-l}}`, `{{ltc-l}}`, `{{och-l}}`, `{{he-m}}`, … | `{{zh-l\|茶}}` | yes (different argument layout) | yes |
| L8 | Other namespaces and projects: `w:` Wikipedia, `Appendix:` (e.g. Arabic roots), Wikispecies via `{{taxlink}}` | `w:light amplification by…` | partly | yes, with a namespace prefix in the target |

wiktextract's English extractor skips these templates when it builds
`etymology_templates`: `mention`, `m`, `m-self`, `link`, `l`, `ll`, `l-self`, and
everything matching `cite-…`, `R:…`, `RQ:…` (the `ignored_etymology_templates`
list in `wiktextract/extractor/en/page.py`). That's why L5 and L6 can only be
recovered from `etymology_links`.

*Measured* on 35 current English entries (550 placed links): 372 links were in
the etymology tree, 178 in the prose. 61 of the 178 prose links (**34%**) came
from L5 or L6 and are invisible in `etymology_templates`. Examples: *salary* →
`salārius`, `sal`, `argentum`; *okay* → `OK` (the entry has no templates at all);
*liar* → `lie`, `-ar`; *quiz* → `question`, `inquisitive`.

---

## 2. The JSON fields involved

```jsonc
{
  "word": "fader", "lang": "English", "lang_code": "en", "pos": "noun",
  "etymology_number": "1",                       // string; present only if the page has several etymologies
  "etymology_text": "Etymology tree\nEnglish fade\n...\nEnglish fader\nFrom fade (verb) + -er.",
  "etymology_links": [                           // NEW dumps only: [display text, target], in text order
    ["fade",     "fade#English"],
    ["*-eh₂",    "Reconstruction:Proto-Indo-European/-eh₂"],
    ["-ārius",   "-arius#Latin:_adjective"],
    ["*-ārijaz", "Reconstruction:Proto-Germanic/-ārijaz#Proto-Germanic:_noun"],
    ["-er",      "-er#English:_agent_noun"],
    ["fade",     "fade#English"],
    ["-er",      "-er#English:_agent_noun"]
  ],
  "etymology_templates": [                       // name + args (all values strings) + expansion
    { "name": "ety",
      "args": { "1": "en", "id": "agent noun", "2": ":af", "3": "fade<pos:verb>", "4": "-er<id:agent noun>", "text": "+", "tree": "1" },
      "expansion": "…" }
  ]
}
```

Facts about `etymology_links` (*measured* and from the PR):

- Each item is a **2-element array `[display, target]`**. There are no named keys.
- Items are in **source order**: tree nodes first (top to bottom), then prose.
- **Repeats are kept**. `fade` and `-er` appear twice in *fader*: once in the tree, once in the prose.
- `display` is exactly the text shown, including `*` for reconstructions and
  diacritics (`-ārius`, `παιδείᾱ`).
- `target` is the **real page name plus anchor**, as Wiktionary computed it. Never
  derive the target from the display text yourself.
- Not collected: `<ref>` contents, category links, empty interwiki targets (`w:`
  with nothing after it), and links from later sections (Derived terms,
  Translations, …).

---

## 3. Finding the link text inside `etymology_text`

`etymology_links` gives no character positions. Recover them with a **forward
scan**. Fall back to the earliest unused occurrence when a link comes out of
order (S14):

```python
def place_links(text, links):
    """[(start, end, display, target)] sorted by position, plus links that could not be placed."""
    placed, unplaced, used, cursor = [], [], [], 0

    def free(a, b):
        return all(b <= s or a >= e for s, e in used)

    def find_free(start):
        at = text.find(display, start)
        while at >= 0 and not free(at, at + len(display)):
            at = text.find(display, at + 1)
        return at

    for display, target in links:
        at = find_free(cursor)
        if at >= 0:
            cursor = at + len(display)           # normal case: next occurrence after the previous link
        else:
            at = find_free(0)                    # S14: out of order, take the earliest unused occurrence
            if at < 0:
                unplaced.append((display, target))   # S7: not visible in the text
                continue
        used.append((at, at + len(display)))
        placed.append((at, at + len(display), display, target))
    placed.sort()
    return placed, unplaced
```

*Measured*:

| Data | Links | Forward scan only | With out-of-order fallback |
|---|---|---|---|
| 79 current kaikki.org entries (39 English words, with Arabic, Hebrew, Greek, Akkadian, Japanese, Chinese etymologies) | 946 | 945 (99.9%) | 945 |
| 30,000 random lines of the 2026-10-25 dump, all languages | 55,172 | 54,912 (99.53%) | **55,012 (99.71%)** |

The 160 links still unplaced are S7 cases (hidden tree nodes such as `ka` and
`Ptah` in Czech *Egypt*), or repeats of a display text that occurs fewer times
in the text than in the link list.

Do **not** use "find anywhere" or a dictionary of display → target. The same
display text often maps to different targets in one entry. In *bass*,
`base` → `base#Old_French`, `base#Middle_English`, `base#English:_basis` and
`base#English`. Only the forward scan pairs each occurrence with the right
target.

### Scenarios to handle when placing links

| # | Scenario | Example | Handling |
|---|---|---|---|
| S1 | Plain prose link | "From **fade** (verb) + **-er**." | Forward scan. |
| S2 | Same display text several times, different targets | *bass*: `base` ×4 | Forward scan (order is the key). |
| S3 | Link inside the flattened **etymology tree** | `Proto-Germanic *-ārijaz` line | Forward scan works. Split the tree from the prose for display (S9). |
| S4 | Reconstructed form: display has `*`, text has `*` | `*leugārijaz` | Forward scan (display includes `*`). |
| S5 | Display text is a different form than the target page | `is` → `wesan#Old_English`, `grātiīs` → `gratia#Latin`, `اَلْكُحْل` → `كحل#Arabic`, Akkadian `𒎎𒋆𒁉𒍣𒁕` → `guḫlum#Akkadian:_antimony` | Search for the **display**, link to the **target**. Never reverse it. |
| S6 | Multi-word display | `ἐγκύκλιος παιδείᾱ`, `light amplification by stimulated emission of radiation` | Forward scan (plain substring). |
| S7 | Link whose text is **not** in `etymology_text` | *bass*: `smallmouth bass` (from an image caption or hidden element) | Return it under `unplaced` and don't fail. |
| S8 | Very short or common display (`is`, `a`, `-o`) could match too early inside another word | `is` in "this" | Rare with the forward scan because links are dense, but you can require a non-letter on both sides of the match and fall back to a plain match if that fails. |
| S9 | Tree block and prose in one string | `"Etymology tree\n…\nEnglish fader\nFrom fade…"` | See 3.1. |
| S10 | Entry has links but no text, or text but no links | *nice* (Unix sense): text, no links | Return the text as one unlinked segment. |
| S11 | Several JSON lines (parts of speech) share the same etymology | *bass* adj and noun under Etymology 1 | The text and links are identical; deduplicate by `(etymology_number, etymology_text)`. |
| S12 | Links duplicated because the tree repeats a prose term | *fader*: `fade` in tree and prose | Keep both; each sits at its own position. |
| S13 | Non-lemma form: the text only points to the lemma | "See the etymology of the corresponding lemma form." (549 of 872 link-less texts in the sample) | No links. Follow `senses[].form_of` to the lemma and show its etymology. |
| S14 | Links listed in a different order than the flattened tree prints them (big trees) | *tabloid* (26 of 28), *quarrel* (16 of 21), Tagalog *January* | Out-of-order fallback in `place_links`. |

### 3.1 The flattened etymology tree

When the page draws a tree (`{{ety|…|tree=1}}`), wiktextract puts its labels at
the start of `etymology_text`:

```
Etymology tree                       <- always the first line
Proto-Indo-European *-yós            <- one node per line: "<Language name> <term>"
Proto-Italic *-āzios
Latin -āriusnom.                     <- relation abbreviation GLUED to the term (no space)
Latin -āriusbor.
Proto-Germanic *-ārijaz
…
▲                                    <- branch-merge marker (a second branch follows)
Italian bassoinflu.
English fader                        <- the entry itself = last tree line
From fade (verb) + -er.              <- prose starts here
```

**Where the tree ends** (*measured*): the tree's last line is the entry itself,
`"<lang> <word>"`. Comparing both sides without diacritics and leading `*`
finds it in **99%** of tree texts (1,980 of 1,999). The failures are trees where
the entry line is absent; treat the whole text as prose there.

```python
def split_tree(text, lang, word):
    if not text.startswith('Etymology tree\n'):
        return None, text
    lines = text.split('\n')
    me = plain(f'{lang} {word.lstrip("*")}')
    for i in range(1, len(lines)):
        if plain(lines[i].replace(' *', ' ', 1)) == me:
            return lines[1:i + 1], '\n'.join(lines[i + 1:])
    return None, text                      # no self line: show everything as prose
# plain() = NFD, then drop combining marks
```

**Relation abbreviations glued to tree nodes** (`keyword_abbrev` values taken
from Wiktionary's tree data in the dump, with counts). Strip them from the node
text and show them as the edge label. A trailing `?` means uncertain
(`der.?`, `bor.?`). Inherited steps have **no** abbreviation.

| Abbreviation | Meaning | Count |
|---|---|---|
| `der.` | Derived from | 1266 |
| `bor.` | Borrowed from | 743 |
| `influ.` | Influenced by | 201 |
| `lbor.` | Learned borrowing from | 153 |
| `nom.` | (Masculine / Neuter) nominalization of | 50 |
| `sl.` | Semantic loan from | 20 |
| `calq.` | Calque of | 11 |
| `deverb.` | Deverbal from | 10 |
| `bf.` | Back-formation from | 8 |
| `redup.` | Reduplication of | 8 |
| `clip.` | Clipping of | 7 |
| `denom.` | Denominal verb from | 2 |
| `vṛd.` | Vṛddhi derivative of | 2 |
| `pcalq.` | Partial calque of | 2 |
| `ellip.` | Ellipsis of | 1 |
| `ubor.` | Unadapted borrowing from | 1 |
| `acronym` | Acronym of (no dot) | 1 |

This list comes from a 60,000-line sample; rarer keywords exist. Match the
longest known abbreviation (plus optional `?`) at the end of the line. Because
the abbreviation is glued on, a term that itself ends in such letters is
ambiguous. When the line has a link placed on it, the link's display gives the
exact term boundary, so anything after the display on that line is the
abbreviation.

For example, `Latin -āriusnom.` with link display `-ārius` gives term `-ārius`
and abbreviation `nom.`.

---

## 4. From link target to Wiktionary URL and to our own data

### 4.1 Grammar of a target

```
target    := page [ "#" anchor ]
page      := [ namespace ":" ] title
anchor    := LanguageName [ ":_" id ]          spaces → "_" in both parts
```

*Measured* shapes (946 links):

| Shape | Count | Example |
|---|---|---|
| main page `#Language` | 517 | `كحل#Arabic`, `fade#English` |
| `Reconstruction:` with `#Language:_id` | 144 | `Reconstruction:Proto-Germanic/-ārijaz#Proto-Germanic:_noun` |
| `Reconstruction:` **without** anchor | 131 | `Reconstruction:Proto-Indo-European/gʷem-` |
| main page `#Language:_id` | 113 | `-er#English:_agent_noun`, `base#English:_basis` |
| main page **without** anchor | 37 | `question`, `univerbation`, `kohl` (plain `[[…]]` links, scenario R8) |
| `w:` (Wikipedia) | 2 | `w:light amplification by stimulated emission of radiation` |
| `Appendix:` | 2 | `Appendix:Arabic roots/ط و ف` |

### 4.2 Building the Wiktionary URL

```
https://en.wiktionary.org/wiki/<page with spaces → "_", percent-encoded>#<anchor>
```

- `w:Foo` → `https://en.wikipedia.org/wiki/Foo`.
- Other interwiki prefixes (`species:`, `wikipedia:`, `s:` …) map to their projects the same way.
- Keep `Reconstruction:`, `Appendix:`, `Thesaurus:`, `Citations:` as part of the
  Wiktionary page name.

### 4.3 Resolving a target to an entry in our data (in-app link)

The dump stores each entry under its **page title**, and reconstructions
**without** `*` (*measured*: `legere`, not `lēgere`; Proto-Germanic `-ārijaz`,
never `*-ārijaz`; 0 of 44,648 proto-language entries start with `*`). So:

| Target | `word` to look up | `lang_code` | Notes |
|---|---|---|---|
| `title#Language…` | `title` (exact) | language name → code via the `language` table (`_` → space) | Main case. |
| `Reconstruction:Language/term#…` | `term` (after the first `/`) | from `Language` in the path | `term` keeps its own hyphens (`-ārijaz`). |
| `title` (no anchor) | `title` | **unknown**: try the citing entry's language first, then any language | R8. Plain `[[links]]` also hit non-dictionary words (`et al.`, `q.v.`). |
| `Appendix:…`, `w:…`, other namespaces | none | none | External link only. |

Then pick the entry or sense using the id after `:_`, if there is one. Turn `_`
back into spaces first.

- **Etymology id.** The entry whose `ety` or `etymon` template has `args.id ==
  id`. Example: `burger` with id `hamburger` or `burgher` picks between the two
  etymologies.
- **Sense id.** A sense whose `senseid` list contains `"<lang_code>:<id>"`, e.g.
  `"en:gum"`.
- If neither matches, use the first entry, as `etymology_walk.py` does.

The anchor names the **full language**. Etymology-only varieties don't have
their own entries, so they never appear in anchors (*measured*):

| Shown in text | Target section |
|---|---|
| Anglo-Norman `salarie` | `salarie#Old_French` |
| New Latin `encyclopaedīa` | `encyclopaedia#Latin` |
| Old Latin `-ios` | `-ios#Latin:_suffix_forming_adjectives` |
| Koine Greek `ἐγκύκλιος παιδείᾱ` | `ἐγκύκλιος παιδεία#Ancient_Greek` |

So the language you show (from the text) and the language you look up (from the
anchor) can differ. Take the lookup language from the anchor.

### 4.4 Resolution scenarios

| # | Scenario | Example | Rule |
|---|---|---|---|
| R1 | Normal term | `fade` → `fade#English` | `word = page`, `lang` from anchor. |
| R2 | Diacritics in display, stripped in page (per-language rules, section 5) | `-ārius` → `-arius#Latin`; `كُحْل` → `كحل#Arabic`; `אָמֵן` → `אמן#Hebrew`; `παιδείᾱ` → `παιδεία#Ancient_Greek` (accent kept); `sōþlīċe` → `soþlice#Old_English` | Use the target. |
| R3 | Reconstruction | `*-ārijaz` → `Reconstruction:Proto-Germanic/-ārijaz#…` | Namespace path gives language and term. |
| R4 | Reconstruction with no anchor | `*gʷem-` → `Reconstruction:Proto-Indo-European/gʷem-` | Same; there's no id. |
| R5 | Anchor with etymology or sense id | `-er#English:_agent_noun` | Section 4.3 id matching. |
| R6 | Etymology-only language → parent | Anglo-Norman → `#Old_French` | Lookup language from the anchor. |
| R7 | Display is an inflected or variant form; target is the lemma | `is` → `wesan`; `grātiīs` → `gratia` | Use the target. |
| R8 | Plain wikilink, no language | `question`, `univerbation` | Lookup without a language, or guess the citing entry's language. |
| R9 | Different script on the page | Akkadian cuneiform → `guḫlum` | Use the target. |
| R10 | Other namespace or project | `w:…`, `Appendix:Arabic roots/…` | External URL only. |
| R11 | Link to a page that doesn't exist (red link) | e.g. `*leugārijaz` (no entry in our dump) | The target is still valid. Mark `in_dump = false` and still offer the Wiktionary URL. |
| R12 | Link not placed in the text (S7) | `smallmouth bass` | Return it separately. |

---

## 5. How Wiktionary turns a term into a page name

You only need this when there's **no** `etymology_links` field (old dumps, or
your own wikitext parsing). It's what Wiktionary's `Module:links` and
`Module:languages` do (*Wiktionary docs*):

1. **Inline modifiers come off first**: `term<t:gloss><id:x><alt:shown><pos:…><tr:…><ts:…><lit:…><g:…><q:…><qq:…><lang:xx><sc:…><ref:…>`.
   The link uses `term`. `<alt:…>` changes only what's displayed. An empty term
   with `<alt:…>` produces **no link**.
2. **A language prefix** `xx:term` (in `ety`/`etymon` and affix parts) or `langN=`
   overrides the template's language for that part.
3. **Embedded wikilinks** in the term: `[[target|shown]]` links `target`;
   `the [[word]]` links `word#Language`.
4. **Special prefixes**:
   - leading `*` → `Reconstruction:<Language name>/<term without *>`
   - leading `!!` → main-namespace entry of a reconstructed language (the "anti-asterisk")
   - leading `:` → link to the text exactly as written, with no stripping
   - `^` only affects transliteration capitals
5. **Per-language `entry_name` rules** (`Module:languages` data): substitute
   characters (`from` → `to`), then remove a language-specific set of marks
   (`remove_diacritics`), keeping listed exceptions. Rules can be per script, and
   etymology-only languages inherit their parent's rules. Observed effects:

   | Language | Removed |
   |---|---|
   | Latin | macrons, breves |
   | Old English | macrons, dots on ċ ġ (keeps æ þ ð) |
   | Ancient Greek | macrons, breves only (accents and breathings stay) |
   | Arabic, Persian | harakat (short-vowel marks), tatweel |
   | Hebrew | niqqud (vowel points) |
   | Russian, Ukrainian, Belarusian | stress marks |
   | Serbo-Croatian, Slovene, Lithuanian | tone and accent marks |

   A generic "strip every combining mark" (`undiacritic()` in our scripts) is
   **wrong** for languages that keep some marks, Ancient Greek accents above all.
6. **Trailing punctuation** is removed (`? ! ; ؟ । ॥ ။ ། …`), and so are a leading `¿ ¡`.
7. **Etymology-only languages** link to their full parent language's section
   (`Module:etymology languages/data` defines the parents). Vulgar Latin `*`
   terms go to `Reconstruction:Latin/…`.
8. **Appendix-constructed languages** (most constructed languages) live at
   `Appendix:<Language>/<term>` and are written without `*`.
9. **Affix hyphens**: in languages that don't write affixes with hyphens, the
   hyphen typed in `{{affix}}` is removed from both display and link. Hebrew and
   some others use their own hyphen character.
10. **Titles Wiktionary can't use** (`# < > [ ] | { }`, a leading `:`, and others)
    become `Unsupported titles/<name>`. Very large pages are split, e.g.
    `a/languages A to L`.
11. **Anchor**: `#<Language name>` (spaces → `_`). With an `id`, it's
    `#<Language name>:_<id>` (spaces → `_`). Reconstruction pages get an anchor
    only when there's an id.

Because R7 and R9 depend on what the editor typed in a piped link, **no rule set
can fully rebuild targets from display text**. That's the main reason to use
`etymology_links`.

---

## 6. Fallback for dumps without `etymology_links` (our current local dump)

What you can still extract, template by template. These are all the families
that link, with *measured* counts out of 5.32M templates and 1,094 distinct
names in our dump; the top 100 names cover 97.6% of uses.

| Family | Templates (count) | Linked term(s) | Language of the term | Display / gloss |
|---|---|---|---|---|
| Single source | `inh` 597k, `der` 531k, `bor+` 221k, `bor` 214k, `inh+` 118k, `uder` 39k, `lbor` 20k, `calque` 13k, `der+` 8k, `cal` 6k, `ubor` 6k, `clq` 4k, `obor` 3k, `sl` 2k, `semantic loan`, `learned borrowing`, `slbor`, `psm`, `partial calque` | `3` | `2` (`1` is the citing language) | `4` = alt, `5`/`t`/`gloss` = gloss, `tr`, `ts`, `id`, `pos`, `lit` |
| Mention-like | `cog` 728k, `ncog` 14k, `noncog` 7k, `m+` 14k, `clipping` 6k, `deverbal` 4k, `abbrev` 3k, `back-form`, `bf`, `back-formation`, `reduplication`, `rdp`, `contraction` | `2` | `1` | `3` = alt, `4`/`t` = gloss, `tr`, `id`, `pos` |
| Multi-part (affixes) | `af` 365k, `suffix` 213k, `affix` 179k, `compound` 136k, `prefix` 132k, `com` 60k, `surf` 48k, `suf` 40k, `com+` 22k, `confix` 15k, `blend` 12k, `pre` 12k, `infix`, `univerbation`, `circumfix`, `interfix` | `2`, `3`, `4`, … | `1`, or `langN` / `xx:` prefix per part | `altN`, `tN`, `trN`, `idN`, `posN`, `litN`, inline `<…>` modifiers |
| Multi-term relatives | `doublet` 38k, `dbt` 8k, `piecewise doublet`, `pseudo-loan` (`2` = source language, then terms) | `2`, `3`, … (`pseudo-loan`: `3`, …) | `1` (`pseudo-loan`: `2`) | `tN`, `altN` |
| Etymon syntax | `ety` 454k, `etymon` 53k | `3`, `4`, … in etymon syntax (`xx:term<modifiers>`). Values starting with `:` are relation keywords | `xx:` prefix, else `1` | `<alt:>`, `<t:>`, `<id:>`, `<pos:>`, `<tr:>`; nested `<ety:…>` |
| Roots | `root` 65k | `3`, `4`, … (Reconstruction pages) | `2` | `id`, `idN` |
| Language-specific | `ja-r` 17k, `zh-l` 12k, `ltc-l` 7k, `och-l` 8k, `ko-l` 3k, `vi-l` 2k, `he-m`, `okm-l`, `l/vi/Latn` | usually `1` (`ja-r`: `1` = kanji, `2` = kana, `linkto` overrides) | implied by the template name | varies |
| To other projects | `named-after` (`wplink` → Wikipedia), `coin`/`coinage` (`w` → Wikipedia), `taxlink` (Wikispecies/Wikipedia), `w`, `wp` | – | – | – |
| Not term links | `yesno`, `glossary` (links to `Appendix:Glossary`), `unk`, `unc`, `lit`, `etydate`, `nonlemma`, `categorize`, `dercat` (categories only), `m-g` (gloss formatting), `internationalism`, `onomatopoeic`, `etymid`, `q`, `ref` | – | – | – |

**Etymon relation keywords** (`args["2"]` and later values starting with `:`;
*Wiktionary docs*): `:from` `:der` `:inh` `:uder` `:bor` `:lbor` `:slbor` `:obor`
`:ubor` `:calque`/`:cal`/`:clq` `:pcal` `:sl` `:influence` `:af`/`:affix`
`:afeq` (hidden) `:blend` `:rdp` `:abbr` `:sylabbr` `:acro` `:init` `:meta`
`:clip` `:ellip` `:univ` `:bf` `:apheretic` `:denom` `:deverbal` `:translit`
`:vrd` `:root` (invisible). Keyword modifiers: `<unc>`, `<ref:>`, `<conj:>`, `<surf>`.

`etymology_tree.parse()` already handles the etymon value syntax, and
`etymology_walk.py` (`PLAIN_TEMPLATES`, `split_etymon_args`) implements the
single-source, multi-part and etymon families.

**Placing template links in the text**: a template's display string is
`alt` (or `altN` / `<alt:>`) if given, otherwise the term. Search for it in
`etymology_text` (or inside the template's `expansion` first, then find the
`expansion` in the text). Expansions of `ety` with `tree=1` contain a cut-off
fragment of Wiktionary's tree JSON; ignore everything before the final prose
sentence.

**Limits of the fallback** (*measured* on current data, so this is what the
old dump misses):

- About **one third of prose links** come from `{{m}}`/`{{l}}`/`[[…]]` and
  aren't recoverable at all.
- **Tree links** are only in the cut-off tree JSON fragment, not in `args`.
- **Targets** must be rebuilt with section 5's rules. R7 and R9 can't be.

You can check rebuilt targets against Wiktionary's title lists to tell blue
links from red. `Downloads\enwiktionary-latest-all-titles-in-ns0.gz` is from
2024 and covers only the main namespace. Download fresh lists for **ns0** (main)
and **ns118** (Reconstruction) from `dumps.wikimedia.org/enwiktionary/latest/`.

---

## 7. Suggested API response

```jsonc
GET /etymology?word=fader&lang=en
{
  "word": "fader", "lang_code": "en",
  "etymologies": [
    {
      "etymology_number": "1",
      "id": "agent noun",                                  // ety template args.id, if any
      "tree": [                                            // from section 3.1; null if the page has no tree
        { "lang": "Proto-Germanic", "text": "*-ārijaz", "relation": null,
          "link": { "target": "Reconstruction:Proto-Germanic/-ārijaz#Proto-Germanic:_noun" } },
        { "lang": "Latin", "text": "-ārius", "relation": "nom.", "uncertain": false, "link": { … } },
        { "merge": true }                                  // the ▲ line
      ],
      "text": "From fade (verb) + -er.",
      "segments": [                                        // text cut at placed links (section 3)
        { "text": "From " },
        { "text": "fade",  "link": { "target": "fade#English",           "page": "fade", "lang_code": "en", "id": null,
                                     "url": "https://en.wiktionary.org/wiki/fade#English", "in_dump": true } },
        { "text": " (verb) + " },
        { "text": "-er",   "link": { "target": "-er#English:_agent_noun", "page": "-er", "lang_code": "en", "id": "agent noun",
                                     "url": "https://en.wiktionary.org/wiki/-er#English:_agent_noun", "in_dump": true } },
        { "text": "." }
      ],
      "unplaced_links": []                                 // S7
    }
  ]
}
```

- **One item per distinct etymology:** deduplicate by `(etymology_number, etymology_text)`, as `sharded/etymology_walk.py` does.
- **`in_dump`:** comes from the section 4.3 lookup, so the client can tell in-app links from external-only ones.
- **Shard lookups:** link targets for one etymology span many words. Batch the `in_dump` checks with one query per shard, as `EntryCache.prefetch` does in `sharded/etymology_walk.py`.

---

## 8. Getting `etymology_links` into our data

1. ~~Download a current dump~~ Done on 2026-10-25. The dump is at
   `Databases\raw-wiktextract-data.jsonl\…\raw-wiktextract-data.jsonl` (26.6 GB),
   and the first 2 GB alone has 219,172 lines with `etymology_links`.
2. Rebuild the shards: `python sharded/build_shards.py --recreate --input <new file>`.
   The JSON is stored whole, so nothing else changes.
3. The relational loader (`converter.py`, `db.py`) doesn't know the field.
   It would need a new table (`EtymologyLink(word_id, ord, display, target)`)
   if the relational database is still wanted.

---

## Sources

- wiktextract `src/wiktextract/extractor/en/page.py`: `ignored_etymology_templates`, `parse_etymology` (`link_collector=links` → `etymology_links`).
- wiktextract PR #1709 (2026-09-21) and commit 358f5db ("Exclude empty interwiki targets from etymology links").
- Wiktionary: `Template:link` (mention/link parameters, link-target rules), `Template:affix` (per-part parameters, inline modifiers, hyphen handling), `Template:etymon` (etymon syntax, relation keywords, `tree`/`text`/`id`), `Template:senseid` (anchor format `Language:_id`), `Module:languages` (`stripDiacritics`, `entry_name`, `logicalToPhysical`, reconstructed and appendix-constructed languages, etymology-only parents).
- Measurements: local dump 2026-08-17 (5.32M etymology templates, random line samples of 30k–60k), and current kaikki.org JSON for 39 English words (79 entries, 946 links), fetched 2026-10-27.
