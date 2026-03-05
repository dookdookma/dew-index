from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from .manifest import load_manifest
from .util import ensure_dir


def ocr_available() -> bool:
    return shutil.which("ocrmypdf") is not None


def _should_skip_ocr(source_pdf: Path, ocr_pdf: Path, force_ocr: bool) -> bool:
    if force_ocr or not ocr_pdf.exists():
        return False
    return ocr_pdf.stat().st_mtime >= source_pdf.stat().st_mtime


def run_ocr_for_record(
    record: dict,
    library_dir: Path,
    jobs: int,
    force_ocr: bool = False,
    lang: str | None = None,
) -> str:
    if not ocr_available():
        raise RuntimeError(
            "OCR requested but OCRmyPDF was not found in PATH. "
            "Install OCRmyPDF/Tesseract or skip OCR."
        )

    source_pdf = library_dir / record["source_path"]
    ocr_pdf = Path(record["ocr_path"])
    ensure_dir(ocr_pdf.parent)
    if _should_skip_ocr(source_pdf, ocr_pdf, force_ocr):
        return "skipped"

    cmd = [
        "ocrmypdf",
        "--deskew",
        "--rotate-pages",
        "--clean",
        "--optimize",
        "3",
        "--jobs",
        str(jobs),
        "--output-type",
        "pdf",
    ]
    if lang:
        cmd.extend(["--language", lang])
    cmd.extend([str(source_pdf), str(ocr_pdf)])

    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            f"OCR failed for {source_pdf}: {stderr[:4000]}"
        )
    return "ocrd"


def run_ocr_batch(
    manifest_path: Path,
    library_dir: Path,
    jobs: int | None = None,
    force_ocr: bool = False,
    lang: str | None = None,
) -> dict:
    if not ocr_available():
        raise RuntimeError(
            "OCR requested but OCRmyPDF was not found in PATH. "
            "Install OCRmyPDF/Tesseract or skip OCR."
        )

    workers = jobs or max(1, os.cpu_count() or 1)
    rows = load_manifest(manifest_path)
    summary = {"total": len(rows), "ocrd": 0, "skipped": 0}
    for row in rows:
        status = run_ocr_for_record(
            row,
            library_dir=library_dir,
            jobs=workers,
            force_ocr=force_ocr,
            lang=lang,
        )
        summary[status] += 1
    return summary
