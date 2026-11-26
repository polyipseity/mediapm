# Picard parity gate

This directory pins the behavior mediapm's media tagger borrows from
**Picard 3.0.4**. `vendored/` is the committed, byte-for-byte CI truth: CI
never reaches the network. `driver.py` verifies every vendored file against
`vendored/provenance.json`, extracts exact source segments with Python's `ast`
module, and executes those unmodified segments against in-process stubs from
`stub/`. No vendored module is imported as a whole, so PyQt6/PyQt5 is never
loaded; the driver installs a meta-path guard that makes any such import a
loud failure and asserts this with a negative PyQt6 import attempt on every
run. Python 3.10 or newer is required (the driver uses PEP 604 union types).
The Rust gate and `mediapm-test-support` prefer `python` on Windows and
`python3` elsewhere, falling back to the other name; with neither present the
gate is a hard failure with the exact message
`python3 is required for the Picard parity gate`.

Provenance hashes describe the committed LF bytes. `.gitattributes` does not
pin line endings for the vendored text files, so the driver and the Rust
gate both normalize CRLF and lone CR to LF before hashing. A CRLF checkout
therefore verifies against the same committed hashes without rewriting the
files.

## Source chain

`--refresh` is the only network path. It tries these sources in order and
accepts a candidate only when its sha256 matches provenance:

1. GitHub raw, branch `release-3.0.4`:
   `https://raw.githubusercontent.com/metabrainz/picard/release-3.0.4/<path>`
2. GitHub raw, tag `v3.0.4`:
   `https://raw.githubusercontent.com/metabrainz/picard/v3.0.4/<path>`
3. PyPI sdist `picard==3.0.4` (URL resolved from the PyPI JSON API).
4. Debian/Ubuntu source tarball
   `https://deb.debian.org/debian/pool/main/p/picard/picard_3.0.4.orig.tar.gz`.

Refresh rewrites the vendored bytes and records the URL that actually
supplied each file, with its sha256, in `vendored/provenance.json`. It never
edits vendored source contents. `COPYING.txt` carries Picard's GPL license
text with the same provenance entry.

## What executes

The driver runs Picard's own code for the behaviors the media tagger touches:

- `mbjson.py`: `recording_to_metadata` for `~rating`, and the
  `add_genres_from_node` / `add_user_tags` family for genre and user-tag
  counters.
- `item.py`: `MetadataItem.add_genre`, `add_folksonomy_tag`, and
  `set_genre_inc_params`.
- `track.py`: `TagGenreFilter`, `_genres_to_metadata`,
  `_convert_folksonomy_tags_to_genre`, and the recording `inc` builder.
- `album.py`: the release `inc` builder.
- `collection.py`: `add_release_to_user_collections` for the collection IDs.
- `coverart/providers/caa.py`: approved-only filtering, type matching, and
  `caa_url_fallback_list` size/fallback URL choice: the largest thumbnail no
  wider than the requested size, or the original URL when none qualifies.
- `coverart/image.py` and `util/imagelist.py`: normalized types, image
  ordering, and `to_be_saved_to_tags` one-front embedding.
- `util/__init__.py`: filter-pattern and titlecase helpers.
- `coverart/__init__.py`: vendored context for the one-front setting; the
  executable one-front policy is `ImageList.to_be_saved_to_tags`.

## Fixtures and output

`fixtures/config.json` is the one configuration both sides use;
`fixtures/mb_node.json` and `fixtures/cover_art_images.json` are fixed and
hermetic. The recording node carries user tags (including the filtered
`seen live`) and a rating; the release node carries user collections in both
the mediapm extraction shape (`user-collections`) and Picard's `collections`
shape with the same IDs. Cover-art descriptors cover approved-only rejection,
type exclusion, 500px selection, original-URL and 250px fallback (including a
1200px-only image under a 500px request), priority ordering, and two front
images where only the first is embedded.

The driver writes one canonical JSON object to stdout:

```json
{
  "cover_art": [{ "url": "...", "maintype": "front", "types": ["front"], "comment": "..." }],
  "embedded": [{ "url": "...", "maintype": "front", "types": ["front"], "comment": "..." }],
  "inc": [{ "entity": "recording", "values": ["artists"] }],
  "tags": [["genre", "Rock"], ["~rating", "4"]]
}
```

Tags are `[key, value, ...]` entries. `inc` is restricted to the shared
vocabulary in `fixtures/config.json`: Picard requests many more tokens than
mediapm implements, and this gate compares the tokens both sides own. Live
Cover Art Archive bytes are out of scope by construction; only descriptor
selection is hermetic.

## Rust side

`src/mediapm-test-support/src/picard_contract.rs` runs the driver and parses
its output. Two unit tests build mediapm's answer from the same fixtures:
tag keys/values and both `inc` sets in `musicbrainz.rs`, cover-art selection
in `cover_art.rs`. `scripts/test-runner/src/gates/picard.rs` rechecks the
vendored hashes and canonical JSON after the suite.
