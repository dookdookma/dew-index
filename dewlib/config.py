from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .util import detect_library_dir


@dataclass(frozen=True)
class PathConfig:
    library_dir: Path
    data_dir: Path
    manifest_path: Path
    pages_dir: Path
    chunks_path: Path
    index_dir: Path
    health_report_path: Path

    @classmethod
    def resolve(
        cls,
        library_dir: str | Path | None = None,
        data_dir: str | Path = "data",
        manifest_path: str | Path | None = None,
    ) -> "PathConfig":
        lib = Path(library_dir) if library_dir else detect_library_dir()
        data = Path(data_dir)
        manifest = Path(manifest_path) if manifest_path else data / "manifest.jsonl"
        return cls(
            library_dir=lib,
            data_dir=data,
            manifest_path=manifest,
            pages_dir=data / "pages",
            chunks_path=data / "chunks.jsonl",
            index_dir=data / "index",
            health_report_path=data / "health_report.json",
        )
