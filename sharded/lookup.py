"""Look a word up across the shards and print its entries as JSON.

    # shard URLs come from shard_urls.txt (or SHARD_URLS, or --shard-url)
    python lookup.py run --lang en
    python lookup.py --prefix ru --limit 10
"""

import argparse
import json
import sys

from shardclient import Shards, default_urls


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('word', nargs='?')
    ap.add_argument('--lang', help='lang_code filter, e.g. en')
    ap.add_argument('--prefix', help='autocomplete instead of lookup')
    ap.add_argument('--limit', type=int, default=20)
    ap.add_argument('--shard-url', action='append', default=[])
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding='utf-8')   # IPA and non-Latin scripts on a Windows console

    with Shards(args.shard_url or default_urls()) as shards:
        if args.prefix:
            for word in shards.prefix(args.prefix, args.limit, args.lang):
                print(word)
        elif args.word:
            entries = shards.lookup(args.word, args.lang)
            print(json.dumps(entries, ensure_ascii=False, indent=2))
            print(f'-- {len(entries)} entr{"y" if len(entries) == 1 else "ies"}')
        else:
            ap.error('give a word or --prefix')


if __name__ == '__main__':
    main()
