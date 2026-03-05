from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from .manifest import load_manifest
from .util import ensure_dir


def is_ocrmypdf_available() -> bool:
    return shutil.which("ocrmypdf") is not None


def _ocr_input_output(record: dict, library_root: Path, ocr_dir: Path) -> tuple[Path, Path]:
    source_pdf = library_root / record["source_path"]
    ocr_pdf = ocr_dir / record["ocr_path"]
    return source_pdf, ocr_pdf


def _is_fresh(source_pdf: Path, ocr_pdf: Path) -> bool:
    if not ocr_pdf.exists():
        return False
    return ocr_pdf.stat().st_mtime >= source_pdf.stat().st_mtime


def run_ocr_batch(
    manifest_path: Path,
    library_root: Path,
    ocr_dir: Path,
    jobs: int | None = None,
    force_ocr: bool = False,
    lang: str | None = None,
) -> dict:
    rows = load_manifest(manifest_path)
    summary = {
        "available": is_ocrmypdf_available(),
        "total": len(rows),
        "ocrd": 0,
        "skipped": 0,
        "failed": 0,
        "errors": [],
    }

    if not summary["available"]:
        return summary

    worker_count = jobs or max(1, os.cpu_count() or 1)
    for row in rows:
        source_pdf, ocr_pdf = _ocr_input_output(row, library_root=library_root, ocr_dir=ocr_dir)
        if not source_pdf.exists():
            summary["failed"] += 1
            summary["errors"].append(f"Missing source PDF: {source_pdf}")
            continue

        ensure_dir(ocr_pdf.parent)
        if (not force_ocr) and _is_fresh(source_pdf, ocr_pdf):
            summary["skipped"] += 1
            continue

        cmd = [
            "ocrmypdf",
            "--deskew",
            "--rotate-pages",
            "--clean",
            "--optimize",
            "0",
            "--jobs",
            str(worker_count),
            "--output-type",
            "pdf",
        ]
        if lang:
            cmd.extend(["--language", lang])
        cmd.extend([str(source_pdf), str(ocr_pdf)])

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            summary["ocrd"] += 1
        else:
            summary["failed"] += 1
            err = (result.stderr or result.stdout or "").strip()
            summary["errors"].append(f"{row['doc_id']} ({row['source_path']}): {err[:400]}")

    return summary
