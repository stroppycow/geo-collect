import logging
import shutil
from pathlib import Path
from typing import Protocol

logger = logging.getLogger(__name__)


class _OutputPathsWithRaw(Protocol):
    @property
    def raw_entities(self) -> Path: ...


class _DownloadRequest(Protocol):
    """Structural type of the supplier requests (`RequestCOG`,
    `RequestLaPosteHexasmal`, `RequestAdminExpressCog`): they all expose the
    raw output path, a description and a ``send()`` download method."""

    @property
    def output_paths(self) -> _OutputPathsWithRaw: ...

    @property
    def description(self) -> str: ...

    def send(self) -> None: ...


def send_or_use_offline_raw(
    request: _DownloadRequest, offline_raw_dir: Path | None
) -> None:
    """Download the raw file of a request, or — in offline mode — copy the
    pre-downloaded raw file from ``offline_raw_dir`` instead.

    In offline mode, ``offline_raw_dir`` is the source sub-directory of the
    offline data directory (``<offline-data-directory>/<source>/``, e.g.
    ``<dir>/adminexpress/``) and the file is looked up by name. A missing file
    fails loudly.
    """
    if offline_raw_dir is None:
        request.send()
        return
    source = offline_raw_dir / request.output_paths.raw_entities.name
    if not source.is_file():
        raise RuntimeError(
            f"Offline mode: raw file '{request.output_paths.raw_entities.name}' "
            f"for {request.description} not found in '{offline_raw_dir}'"
        )
    target = request.output_paths.raw_entities
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    shutil.copyfile(source, target)
    logger.info(
        f"Offline mode: using provided raw file '{source}' for {request.description}"
    )
