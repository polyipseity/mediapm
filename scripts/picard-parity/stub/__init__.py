"""In-process stand-ins for the Picard runtime used by the parity driver.

The vendored files are never imported as modules: the driver extracts exact
function/class source segments and compiles them into a namespace built here.
That is what keeps PyQt out of the process while still executing Picard's own
code. Every object below exists only to answer the calls those segments make.
"""

from __future__ import annotations

import sys
from collections import Counter, OrderedDict, defaultdict, namedtuple
from collections.abc import MutableSequence
from types import ModuleType, SimpleNamespace
from typing import Any


class QUrl:
    """Minimal URL value used by Picard's extracted image classes.

    This deliberately shadows `PyQt6.QtNetwork.QUrl`; no Qt package is
    installed or imported by the parity gate.
    """

    def __init__(self, value: str) -> None:
        self._value = value

    def toString(self) -> str:  # noqa: N802 - Picard calls Qt's spelling
        """Returns the URL text Picard would read from a Qt URL."""
        return self._value


class StubLog:
    """No-op replacement for `picard.log`."""

    def debug(self, *args: Any, **kwargs: Any) -> None:
        """Accepts Picard debug calls without producing gate output."""

    def debug_if(self, *args: Any, **kwargs: Any) -> None:
        """Accepts Picard conditional debug calls."""

    def error(self, *args: Any, **kwargs: Any) -> None:
        """Accepts Picard error calls; the gate fails on its own verdict."""

    def warning(self, *args: Any, **kwargs: Any) -> None:
        """Accepts Picard warning calls."""


class StubConfig:
    """`config.setting` / `config.persist` view over the fixture config."""

    def __init__(self, settings: dict[str, Any], persist: dict[str, Any] | None = None) -> None:
        self.setting = settings
        self.persist = persist or {}


class StubMetadata:
    """The subset of Picard's Metadata mapping the extracted code touches."""

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}
        self.length = 0

    def add_unique(self, key: str, value: Any) -> None:
        """Records a value for a key that has not been set yet."""
        self._values.setdefault(key, value)

    def getall(self, key: str) -> list[Any]:
        """Returns one value for key as Picard's list-valued getter does."""
        value = self._values.get(key)
        return [] if value is None else [value]

    def __getitem__(self, key: str) -> Any:
        return self._values.get(key, "")

    def __setitem__(self, key: str, value: Any) -> None:
        self._values[key] = value


class StubUserCollection:
    """A user collection record created by Picard's vendored helper."""

    def __init__(self, collection_id: str) -> None:
        self.id = collection_id
        self.name = ""
        self._size = 0
        self.releases: set[str] = set()
        self.pending_releases: set[str] = set()

    @property
    def size(self) -> int:
        """Returns the recorded release count."""
        return self._size

    @size.setter
    def size(self, value: int | str) -> None:
        self._size = int(value)


def install_stub_modules() -> None:
    """Provides the one lazy import the extracted imagelist segment performs."""
    for name in ("picard", "picard.coverart", "picard.coverart.processing"):
        sys.modules[name] = ModuleType(name)
    filters = ModuleType("picard.coverart.processing.filters")
    filters.filter_image_for_file = lambda image, previous: True
    sys.modules["picard.coverart.processing.filters"] = filters


def stub_environment(settings: dict[str, Any]) -> dict[str, Any]:
    """Builds the globals for every extracted vendored segment.

    `settings` is the shared fixture config; the same object is handed to
    Picard's `get_config()` calls and to the cover-art settings argument.
    """
    log = StubLog()
    config = StubConfig(settings, {"oauth_username": settings["oauth_username"]})
    picard = SimpleNamespace(log=log)
    picard_util = SimpleNamespace(format_time=lambda _ms, _display_zero=False: "0:00")
    return {
        "Counter": Counter,
        "OrderedDict": OrderedDict,
        "MutableSequence": MutableSequence,
        "defaultdict": defaultdict,
        "namedtuple": namedtuple,
        "re": __import__("re"),
        "unicodedata": __import__("unicodedata"),
        "Any": Any,
        "SimpleNamespace": SimpleNamespace,
        "log": log,
        "get_config": lambda: config,
        "N_": lambda text: text,
        "_": lambda text: text,
        "picard": picard,
        "picard_util": picard_util,
        "format_time": picard_util.format_time,
        "QUrl": QUrl,
        "Id3ImageType": lambda value=None: value,
        "image_type_as_id3_num": lambda _maintype: 3,
        "translated_types_as_string": lambda types, separator: separator.join(types),
        "TYPES_SEPARATOR": ", ",
        "SettingConfigSection": dict,
        "Metadata": StubMetadata,
        "StubMetadata": StubMetadata,
        "StubUserCollection": StubUserCollection,
        "tagger_instance": lambda: SimpleNamespace(mb_api=SimpleNamespace()),
        "run_image_metadata_filters": lambda _image_data: True,
        "DebugOpt": SimpleNamespace(COVERART="coverart"),
    }
