#!/usr/bin/env python3
"""Hermetic Picard 3.0.4 parity oracle for the media tagger.

The committed files under ``vendored/`` are CI truth. This driver verifies
their sha256 hashes, extracts exact source segments with ``ast``, and executes
those unmodified segments against in-process stubs. It never imports the
vendored modules as a whole, and a meta-path guard makes any PyQt import an
explicit error. No network access happens unless ``--refresh`` is passed.

Refresh follows the documented source chain in order:

1. GitHub raw ``release-3.0.4``;
2. GitHub raw tag ``v3.0.4``;
3. the PyPI sdist for ``picard==3.0.4``;
4. the Debian/Ubuntu source tarball.

A candidate is accepted only when its sha256 matches provenance.json; refresh
rewrites the vendored bytes and records the URL that actually supplied them.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import http.client
import importlib
import io
import json
import sys
import tarfile
import textwrap
import urllib.error
import urllib.request
from importlib.abc import MetaPathFinder
from pathlib import Path
from types import SimpleNamespace
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
VENDORED = HERE / "vendored"
FIXTURES = HERE / "fixtures"
PROVENANCE = VENDORED / "provenance.json"
sys.path.insert(0, str(HERE))
# Importing the local stub must not leave __pycache__ in the repository.
sys.dont_write_bytecode = True

from stub import install_stub_modules, stub_environment  # noqa: E402 - local stub package


class QtImportBlocker(MetaPathFinder):
    """Turns any attempted PyQt import into a loud gate failure."""

    def find_spec(self, fullname: str, path: Any = None, target: Any = None) -> None:
        """Refuses PyQt5/PyQt6 and leaves every other import alone."""
        if fullname.split(".", 1)[0] in {"PyQt5", "PyQt6"}:
            raise ImportError(f"the Picard parity gate must not import {fullname}")
        return None


sys.meta_path.insert(0, QtImportBlocker())


def _sha256(data: bytes) -> str:
    """Hashes text with host-independent line endings.

    The committed provenance hashes describe LF bytes. A CRLF or CR checkout
    must verify against the same hashes, so normalization happens before every
    hash, including the refresh candidate check.
    """
    return hashlib.sha256(data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")).hexdigest()


def verify_qt_import_blocker() -> None:
    """Negative test: the guard must refuse an attempted PyQt6 import."""
    try:
        importlib.import_module("PyQt6")
    except ImportError as error:
        expected = "the Picard parity gate must not import PyQt6"
        if expected not in str(error):
            raise SystemExit(f"Qt import blocker gave an unexpected error: {error}") from error
        return
    raise SystemExit("the Picard parity gate imported PyQt6 despite its import blocker")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def verify_vendored() -> dict[str, Any]:
    """Checks every vendored file against its recorded sha256."""
    provenance = _load_json(PROVENANCE)
    for entry in provenance["files"]:
        path = VENDORED / entry["path"]
        if not path.is_file():
            raise SystemExit(f"vendored Picard source is missing: {entry['path']}")
        actual = _sha256(path.read_bytes())
        if actual != entry["sha256"]:
            raise SystemExit(
                f"vendored Picard source hash mismatch for {entry['path']}: "
                f"expected {entry['sha256']}, got {actual}"
            )
    return provenance


# --- Refresh (the only network path) -------------------------------------------------


def _fetch(url: str) -> bytes:
    """Fetches one URL, retrying transient network interruptions."""
    last_error: Exception | None = None
    for _attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                return response.read()
        except (OSError, urllib.error.URLError, http.client.HTTPException) as error:
            last_error = error
    assert last_error is not None
    raise last_error


def _archive_members(url: str, prefix: str) -> dict[str, bytes]:
    """Extracts one source tarball into vendored-path -> bytes."""
    with tarfile.open(fileobj=io.BytesIO(_fetch(url)), mode="r:gz") as archive:
        members = {}
        for member in archive.getmembers():
            if not member.isfile():
                continue
            name = member.name
            if name.startswith(prefix):
                name = name[len(prefix) :]
            members[name] = archive.extractfile(member).read()
        return members


def _sdist_members() -> tuple[str, dict[str, bytes]]:
    metadata = json.loads(_fetch("https://pypi.org/pypi/picard/3.0.4/json"))
    url = next(
        file["url"] for file in metadata["urls"] if file["packagetype"] == "sdist"
    )
    return url, _archive_members(url, "picard-3.0.4/")


def _debian_members() -> tuple[str, dict[str, bytes]]:
    url = "https://deb.debian.org/debian/pool/main/p/picard/picard_3.0.4.orig.tar.gz"
    return url, _archive_members(url, "picard-3.0.4/")


def refresh() -> None:
    """Refreshes vendored bytes and provenance through the four-source chain."""
    provenance = verify_vendored()
    sdist: tuple[str, dict[str, bytes]] | None = None
    debian: tuple[str, dict[str, bytes]] | None = None
    for entry in provenance["files"]:
        rel = entry["path"]
        candidates: list[tuple[str, bytes | None]] = [
            (f"https://raw.githubusercontent.com/metabrainz/picard/release-3.0.4/{rel}", None),
            (f"https://raw.githubusercontent.com/metabrainz/picard/v3.0.4/{rel}", None),
        ]
        candidates.append(("pypi sdist picard==3.0.4", None))
        candidates.append(("Debian/Ubuntu picard 3.0.4 source", None))
        chosen: tuple[str, bytes] | None = None
        for label, data in candidates:
            try:
                if label.startswith("https://raw.githubusercontent.com"):
                    data = _fetch(label)
                elif label.startswith("pypi sdist"):
                    if sdist is None:
                        sdist = _sdist_members()
                    label, members = sdist
                    data = members.get(rel)
                else:
                    if debian is None:
                        debian = _debian_members()
                    label, members = debian
                    data = members.get(rel)
                if data is not None and _sha256(data) == entry["sha256"]:
                    chosen = (label, data)
                    break
            except (
                OSError,
                http.client.HTTPException,
                KeyError,
                StopIteration,
                tarfile.TarError,
                ValueError,
            ):
                continue
        if chosen is None:
            raise SystemExit(f"refresh could not verify {rel} against any source in the chain")
        label, data = chosen
        (VENDORED / rel).parent.mkdir(parents=True, exist_ok=True)
        (VENDORED / rel).write_bytes(data)
        entry["source_url"] = label
        print(f"{rel}: {label}")
    PROVENANCE.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


# --- Vendored source extraction ------------------------------------------------------

_SOURCES: dict[str, str] = {}


def source(rel: str) -> str:
    """Reads one vendored source file once."""
    if rel not in _SOURCES:
        _SOURCES[rel] = (VENDORED / rel).read_text(encoding="utf-8")
    return _SOURCES[rel]


def _body(tree: ast.Module, class_name: str | None = None) -> list[ast.stmt]:
    if class_name is None:
        return tree.body
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node.body
    raise SystemExit(f"vendored class not found: {class_name}")


def node_named(rel: str, name: str, class_name: str | None = None) -> ast.stmt:
    """Finds one top-level or class-level AST node by name."""
    nodes = _body(ast.parse(source(rel)), class_name)
    for node in nodes:
        if getattr(node, "name", None) == name:
            return node
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            return node
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
        ):
            return node
    raise SystemExit(f"vendored source segment not found: {rel}:{name}")


def segment(rel: str, name: str, class_name: str | None = None) -> str:
    """Returns the exact source text of one node, decorators included."""
    text = source(rel)
    node = node_named(rel, name, class_name)
    decorators = [
        "@" + ast.get_source_segment(text, decorator)
        for decorator in getattr(node, "decorator_list", [])
    ]
    body = ast.get_source_segment(text, node)
    return "\n".join([*decorators, body])


def compile_segments(namespace: dict[str, Any], label: str, code: str) -> None:
    """Compiles extracted source only, with deferred annotations."""
    compiled = "from __future__ import annotations\n" + code
    exec(compile(compiled, label, "exec"), namespace)  # noqa: S102 - vendored source is the oracle


def method_in_class(rel: str, class_name: str, method_name: str, stub_name: str) -> str:
    """Wraps one extracted method in a stub class so no Qt base is evaluated."""
    method = textwrap.indent(
        textwrap.dedent(segment(rel, method_name, class_name)), "    "
    )
    return f"class {stub_name}:\n{method}\n"


def inc_builder(
    rel: str, class_name: str, func_name: str, stop_text: str, stub_name: str
) -> str:
    """Extracts the vendored `inc` construction block from an entity load."""
    function = node_named(rel, func_name, class_name)
    assert isinstance(function, ast.FunctionDef)
    text = source(rel)
    statements = function.body
    start = next(
        index
        for index, stmt in enumerate(statements)
        if isinstance(stmt, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "config"
            for target in stmt.targets
        )
    )
    end = next(
        index
        for index, stmt in enumerate(statements)
        if index > start and stop_text in (ast.get_source_segment(text, stmt) or "")
    )
    block = "\n\n".join(
        segment(rel, getattr(stmt, "name", "")) if hasattr(stmt, "name") else
        (ast.get_source_segment(text, stmt) or "")
        for stmt in statements[start:end]
    )
    body = textwrap.indent(textwrap.dedent(block), "    ")
    return f"def {stub_name}(self):\n{body}\n    return inc\n"


def load_vendored(namespace: dict[str, Any]) -> None:
    """Executes all source segments the parity contract depends on."""
    compile_segments(
        namespace,
        "vendored/picard/util/__init__.py",
        "\n\n".join(
            segment("picard/util/__init__.py", name)
            for name in (
                "iswbound",
                "titlecase",
                "wildcards_to_regex_pattern",
                "pattern_as_regex",
            )
        ),
    )
    compile_segments(
        namespace,
        "vendored/picard/item.py",
        "class MetadataItemStub:\n"
        "    def __init__(self):\n"
        "        self._genres = Counter()\n"
        "        self._folksonomy_tags = Counter()\n"
        "    @property\n"
        "    def genres(self):\n"
        "        return self._genres\n"
        "    @property\n"
        "    def folksonomy_tags(self):\n"
        "        return self._folksonomy_tags\n"
        + textwrap.indent(
            textwrap.dedent(
                "\n".join(
                    segment("picard/item.py", name, "MetadataItem")
                    for name in ("add_genre", "add_folksonomy_tag", "set_genre_inc_params")
                )
            ),
            "    ",
        )
        + "\n",
    )
    compile_segments(
        namespace,
        "vendored/picard/track.py",
        segment("picard/track.py", "TagGenreFilter")
        + "\n\n"
        + method_in_class(
            "picard/track.py", "Track", "_genres_to_metadata", "TrackStub"
        )
        + "\n\n"
        + textwrap.indent(
            textwrap.dedent(
                segment("picard/track.py", "_convert_folksonomy_tags_to_genre", "Track")
            ),
            "    ",
        ),
    )
    compile_segments(
        namespace,
        "vendored/picard/mbjson.py",
        "\n\n".join(
            segment("picard/mbjson.py", name)
            for name in (
                "_RECORDING_TO_METADATA",
                "_node_skip_empty_iter",
                "recording_to_metadata",
                "add_genres_from_node",
                "add_genres",
                "add_user_genres",
                "add_tags",
                "add_user_tags",
            )
        ),
    )
    compile_segments(
        namespace,
        "vendored/picard/collection.py",
        "\n\n".join(
            (
                segment("picard/collection.py", "user_collections"),
                segment("picard/collection.py", "Collection"),
                segment("picard/collection.py", "get_user_collection"),
                segment("picard/collection.py", "add_release_to_user_collections"),
            )
        ),
    )
    compile_segments(
        namespace,
        "vendored/picard/album.py",
        inc_builder(
            "picard/album.py", "Album", "load", "def create_request():", "build_release_inc"
        ),
    )
    compile_segments(
        namespace,
        "vendored/picard/track.py",
        inc_builder(
            "picard/track.py", "NonAlbumTrack", "load", "get_track_by_id", "build_recording_inc"
        ),
    )
    compile_segments(
        namespace,
        "vendored/picard/coverart/image.py",
        "\n\n".join(
            (
                segment("picard/coverart/image.py", "CoverArtImage"),
                segment("picard/coverart/image.py", "CaaCoverArtImage"),
            )
        ),
    )
    compile_segments(
        namespace,
        "vendored/picard/util/imagelist.py",
        segment("picard/util/imagelist.py", "ImageList"),
    )
    compile_segments(
        namespace,
        "vendored/picard/coverart/providers/caa.py",
        "\n\n".join(
            segment("picard/coverart/providers/caa.py", name)
            for name in (
                "CaaSizeItem",
                "CaaThumbnailListItem",
                "_CAA_THUMBNAIL_SIZE_MAP",
                "_CAA_THUMBNAIL_SIZE_ALIASES",
                "caa_url_fallback_list",
                "caa_image_types_accepted",
            )
        )
        + "\n\n"
        + method_in_class(
            "picard/coverart/providers/caa.py",
            "CoverArtProviderCaa",
            "_caa_json_downloaded",
            "CaaProviderStub",
        ),
    )


# --- Contract construction -----------------------------------------------------------


def build_contract(config: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    """Runs the extracted Picard code and returns the canonical contract."""
    install_stub_modules()
    namespace = stub_environment(config)
    load_vendored(namespace)

    recording = node["recording"]
    release = node["release"]

    item = namespace["MetadataItemStub"]()
    namespace["add_genres_from_node"](recording, item)
    track = namespace["TrackStub"]()
    track.metadata = namespace["StubMetadata"]()
    track._merged_folksonomy_tags = item.folksonomy_tags
    track._merged_genres = item.genres
    namespace["TrackStub"]._convert_folksonomy_tags_to_genre(track)
    genres = track.metadata["genre"]

    user_tag_item = namespace["MetadataItemStub"]()
    for entity in (recording, release):
        namespace["add_user_tags"](entity.get("user-tags", []), user_tag_item)

    metadata = namespace["StubMetadata"]()
    namespace["recording_to_metadata"](recording, metadata, None, None)
    rating = metadata["~rating"]

    namespace["add_release_to_user_collections"](release)
    collections = sorted(namespace["user_collections"])

    tags: list[list[str]] = []
    if genres:
        if config["join_genres"]:
            tags.append(["genre", config["join_genres"].join(genres)])
        else:
            tags.append(["genre", *genres])
    if user_tag_item.folksonomy_tags:
        tags.append(["~usertags", *sorted(user_tag_item.folksonomy_tags)])
    if rating != "":
        tags.append(["~rating", str(rating)])
    if collections:
        tags.append(["musicbrainz_collectionid", *collections])

    authorizer = SimpleNamespace(
        webservice=SimpleNamespace(
            oauth_manager=SimpleNamespace(
                is_authorized=lambda: config["oauth_authorized"]
            )
        )
    )
    inc_self = SimpleNamespace(tagger=authorizer)
    inc_self.set_genre_inc_params = namespace["MetadataItemStub"].__dict__[
        "set_genre_inc_params"
    ]
    allowed = set(config["inc_contract_tokens"])
    inc = [
        {
            "entity": entity,
            "values": sorted(set(builder(inc_self)) & allowed),
        }
        for entity, builder in (
            ("recording", namespace["build_recording_inc"]),
            ("release", namespace["build_release_inc"]),
        )
    ]

    provider = namespace["CaaProviderStub"]()
    provider.album = SimpleNamespace(complete_task=lambda _task: None)
    provider.error = lambda message: SystemExit(message)
    provider.metadata = namespace["StubMetadata"]()
    provider.coverartimage_class = namespace["CaaCoverArtImage"]
    provider.coverartimage_thumbnail_class = namespace["CaaCoverArtImage"]
    provider.queued_images = []
    provider.queue_put = provider.queued_images.append
    provider.next_in_queue = lambda: None
    provider.restrict_types = config["restrict_types"]
    provider.included_types = set(config["included_types"])
    provider.excluded_types = set(config["excluded_types"])
    provider.ignore_json_not_found_error = True
    provider._caa_json_downloaded(_load_json(FIXTURES / "cover_art_images.json"), None, False)
    images = sorted(provider.queued_images)
    embedded = list(namespace["ImageList"](images).to_be_saved_to_tags(settings=config))

    def describe(image: Any) -> dict[str, Any]:
        return {
            "url": image.url.toString(),
            "maintype": image.maintype,
            "types": list(image.normalized_types()),
            "comment": image.comment,
        }

    return {
        "tags": sorted(tags),
        "inc": inc,
        "cover_art": [describe(image) for image in images],
        "embedded": [describe(image) for image in embedded],
    }


def main() -> None:
    verify_qt_import_blocker()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="fetch the source chain, verify sha256, and rewrite vendored files",
    )
    args = parser.parse_args()
    if args.refresh:
        refresh()
        return
    verify_vendored()
    config = _load_json(FIXTURES / "config.json")
    node = _load_json(FIXTURES / "mb_node.json")
    contract = build_contract(config, node)
    print(json.dumps(contract, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
