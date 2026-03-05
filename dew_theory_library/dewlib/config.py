from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    library_root: Path
    data_dir: Path
    manifest_path: Path
    pages_dir: Path
    chunks_path: Path
    ocr_dir: Path
    index_dir: Path
    health_report_path: Path

    @classmethod
    def from_args(
        cls,
        library_root: str | Path = "library",
        data_dir: str | Path = "data",
        manifest_path: str | Path | None = None,
    ) -> "Paths":
        lib = Path(library_root)
        data = Path(data_dir)
        manifest = Path(manifest_path) if manifest_path else data / "manifest.jsonl"
        return cls(
            library_root=lib,
            data_dir=data,
            manifest_path=manifest,
            pages_dir=data / "pages",
            chunks_path=data / "chunks.jsonl",
            ocr_dir=data / "ocr",
            index_dir=data / "index",
            health_report_path=data / "health_report.json",
        )
