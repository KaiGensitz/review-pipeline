from __future__ import annotations
from pipeline.core import pipeline as _pipeline

globals().update({name: getattr(_pipeline, name) for name in dir(_pipeline) if not name.startswith("__")})

class FullTextStageMixin:
    def _preparse_full_text_pdfs(self, papers: list[PaperRecord]) -> None:
        """Parse full_text PDFs before screening to warm caches and surface parse status."""

        self._suppress_noisy_parser_library_logs()

        total = len(papers)
        ok_count = 0
        fail_count = 0
        report_rows: list[dict[str, str]] = []

        if not self.quiet:
            print(f"[preparse] Starting full_text preflight parsing for {total} paper(s)...")

        for idx, paper in enumerate(papers, start=1):
            resolved_path = self._resolve_pdf_path(paper)
            text, _page_count, used_path, _pages = self._load_pdf_text(
                paper,
                resolved_path,
                include_pages=False,
            )

            success = bool((text or "").strip())
            if success:
                ok_count += 1
            else:
                fail_count += 1

            parser_level = ""
            if success:
                if self.use_advanced_pdf_parser and self._compact_artifacts_enabled():
                    target_path = used_path or resolved_path
                    if target_path is not None:
                        parser_level = self._read_parser_level_for_folder(Path(target_path).parent)
                elif not self.use_advanced_pdf_parser:
                    parser_level = "Legacy reader"

            report_rows.append(
                {
                    "paper_id": str(paper.paper_id),
                    "status": "OK" if success else "FAIL",
                    "parser_level": parser_level,
                    "pdf_path": str((used_path or resolved_path) or ""),
                }
            )

            if not self.quiet and self._fulltext_preparse_log_each_paper:
                suffix = f" parser='{parser_level}'" if parser_level else ""
                status = "OK" if success else "FAIL"
                print(
                    f"[preparse] {idx}/{total} paper={paper.paper_id} status={status}{suffix}",
                    flush=True,
                )

        self._write_fulltext_preparse_report(report_rows, total, ok_count, fail_count)

        if not self.quiet:
            print(
                f"[preparse] Completed preflight parsing: ok={ok_count} fail={fail_count} total={total}",
                flush=True,
            )

    def _write_fulltext_preparse_report(
        self,
        rows: list[dict[str, str]],
        total: int,
        ok_count: int,
        fail_count: int,
    ) -> None:
        """Write one compact JSON report for full_text preparse outcomes."""

        def _starts_with(level: str, prefix: str) -> bool:
            return bool(level and level.startswith(prefix))

        primary_pymupdf_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_PYMUPDF_FALLBACK)
        )
        fallback_docling_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_DOCLING_SUCCESS)
        )
        fallback_ocr_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_OCR_SUCCESS)
        )
        low_density_without_ocr_count = sum(
            1
            for row in rows
            if _starts_with(str(row.get("parser_level") or ""), PARSER_LEVEL_LOW_DENSITY)
        )
        low_density_trigger_count = fallback_ocr_count + low_density_without_ocr_count
        legacy_reader_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and str(row.get("parser_level") or "") == "Legacy reader"
        )
        unknown_parser_level_count = sum(
            1
            for row in rows
            if str(row.get("status") or "") == "OK"
            and not str(row.get("parser_level") or "").strip()
        )

        parser_outcome_counts = {
            PARSER_LEVEL_PYMUPDF_FALLBACK: primary_pymupdf_count,
            PARSER_LEVEL_DOCLING_SUCCESS: fallback_docling_count,
            PARSER_LEVEL_OCR_SUCCESS: fallback_ocr_count,
            PARSER_LEVEL_LOW_DENSITY: low_density_without_ocr_count,
            "Legacy reader": legacy_reader_count,
            "Unknown parser level": unknown_parser_level_count,
        }
        parser_outcome_counts = {k: v for k, v in parser_outcome_counts.items() if v > 0}

        failures = [
            {
                "paper_id": str(row.get("paper_id") or ""),
                "pdf_path": str(row.get("pdf_path") or ""),
            }
            for row in rows
            if str(row.get("status") or "") == "FAIL"
        ]

        report_payload: dict[str, Any] = {
            "meta": "full_text_preparse_report",
            "schema_version": 2,
            "stage": self.stage,
            "run_label": self.run_label,
            "run_id": self.run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": {
                "papers": total,
                "parsed_ok": ok_count,
                "parsed_fail": fail_count,
            },
            "parser_handler_order": [
                PARSER_LEVEL_PYMUPDF_FALLBACK,
                PARSER_LEVEL_DOCLING_SUCCESS,
                PARSER_LEVEL_LOW_DENSITY,
                PARSER_LEVEL_OCR_SUCCESS,
            ],
            "parser_outcome_counts": parser_outcome_counts,
            "low_text_density_trigger_count": low_density_trigger_count,
            "failures": failures,
        }

        report_path = self.stage_output_dir / f"{self.stage}_preparse_report.json"
        try:
            self.stage_output_dir.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            if not self.quiet:
                print(f"[preparse] Report: {report_path}", flush=True)
        except Exception as exc:  # pylint: disable=broad-except
            if not self.quiet:
                print(f"[warning] Could not write preparse report: {exc}")

    def _read_full_text_normalized_body(self, folder_path: Path) -> str:
        """human readable hint: return just the normalized body text from the sidecar."""

        text = ""
        normalized_path = self._first_existing_prefixed_path(folder_path, "full_text_normalized.txt")
        if normalized_path:
            try:
                text = normalized_path.read_text(encoding="utf-8")
            except Exception:
                return ""
        if not text:
            return ""

        marker = "=== normalized_full_text ==="
        marker_index = text.find(marker)
        return text[marker_index + len(marker):] if marker_index >= 0 else text

    def _has_full_text_normalized_content(self, folder_path: Path) -> bool:
        """human readable hint: treat the normalized text sidecar as valid only when it contains body text."""

        body = self._read_full_text_normalized_body(folder_path)
        return bool(normalize_extracted_text_for_llm(body).strip())

    @staticmethod
    def _full_text_length_ratio_min() -> float:
        """human readable hint: guardrail ratio for normalized length vs direct parser length."""

        try:
            ratio = float(LLM_SETTINGS.get("data_extraction_full_text_length_ratio_min", 0.0) or 0.0)
        except (TypeError, ValueError):
            ratio = 0.0
        return max(0.0, min(1.0, ratio))

    @staticmethod
    def _pdf_ratio_cache_key(pdf_path: Path) -> dict[str, int]:
        """human readable hint: use stable filesystem facts to invalidate ratio-check cache entries."""

        pdf_stat = pdf_path.stat()
        return {
            "pdf_size": int(pdf_stat.st_size),
            "pdf_mtime_ns": int(pdf_stat.st_mtime_ns),
        }

    def _full_text_ratio_artifact_payload(
        self,
        folder_path: Path,
        paper: PaperRecord,
    ) -> tuple[Path, dict[str, Any], FullTextRatioCheckCache | None]:
        """human readable hint: read the durable ratio-check audit from the full-text artifact."""

        artifact_candidates = self._compact_artifact_candidates_for_folder(
            folder_path,
            stage="full_text",
            paper_id=str(paper.paper_id),
        )
        artifact_path = next(
            (candidate for candidate in artifact_candidates if candidate.exists()),
            artifact_candidates[0],
        )
        payload: dict[str, Any] = {}
        if artifact_path.exists():
            try:
                loaded = json.loads(artifact_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    payload = loaded
            except Exception:
                payload = {}
        return artifact_path, payload, FullTextRatioCheckCache.from_artifact_payload(payload)

    def _write_full_text_ratio_check_cache(
        self,
        *,
        artifact_path: Path,
        payload: dict[str, Any],
        paper: PaperRecord,
        folder_path: Path,
        cache_entry: FullTextRatioCheckCache,
    ) -> None:
        """human readable hint: persist a ratio-check result without changing extraction evidence."""

        metadata_snapshot = self._metadata_snapshot_for_folder(folder_path, fallback=paper.metadata)
        payload = dict(payload or {})
        payload.setdefault("meta", "stage_artifact")
        payload.setdefault("schema_version", 1)
        payload.setdefault("stage", "full_text")
        payload.setdefault("paper_id", str(paper.paper_id))
        payload.setdefault("metadata", metadata_snapshot)
        payload["full_text_ratio_check"] = cache_entry.to_artifact_payload()
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _refresh_full_text_normalized_if_ratio_low(
        self,
        paper: PaperRecord,
        folder_path: Path,
        pdf_path: Path,
    ) -> None:
        """human readable hint: re-parse PDFs when cached normalized text looks too short."""

        min_ratio = self._full_text_length_ratio_min()
        if min_ratio <= 0.0:
            return

        normalized_body = self._read_full_text_normalized_body(folder_path)
        normalized_value = normalize_extracted_text_for_llm(normalized_body).strip()
        normalized_len = len(normalized_value)
        if normalized_len <= 0:
            return

        try:
            pdf_cache_key = self._pdf_ratio_cache_key(pdf_path)
        except Exception:
            pdf_cache_key = {}
        normalized_hash = self._sha256_text(normalized_value)
        artifact_path, artifact_payload, cached_ratio = self._full_text_ratio_artifact_payload(folder_path, paper)
        if (
            pdf_cache_key
            and cached_ratio
            and cached_ratio.matches(
                pdf_size=pdf_cache_key["pdf_size"],
                pdf_mtime_ns=pdf_cache_key["pdf_mtime_ns"],
                normalized_text_sha256=normalized_hash,
            )
        ):
            if cached_ratio.ratio < min_ratio and not self.quiet:
                print(
                    "[warning] full_text sanity check ratio below minimum from cache "
                    f"for paper={paper.paper_id} ratio={cached_ratio.ratio:.2f} "
                    f"normalized_len={cached_ratio.normalized_len} direct_len={cached_ratio.direct_len} "
                    f"parser='{cached_ratio.parser_level}'"
                )
            return

        direct_text, parser_level = extract_markdown_from_pdf_with_level(pdf_path)
        direct_len = len((direct_text or "").strip())
        if direct_len <= 0:
            if not self.quiet:
                print(
                    "[warning] full_text sanity check could not extract direct text "
                    f"for paper={paper.paper_id} path={pdf_path}"
                )
            return

        ratio = normalized_len / float(direct_len) if direct_len else 0.0
        if ratio >= min_ratio:
            if pdf_cache_key:
                self._write_full_text_ratio_check_cache(
                    artifact_path=artifact_path,
                    payload=artifact_payload,
                    paper=paper,
                    folder_path=folder_path,
                    cache_entry=FullTextRatioCheckCache(
                        pdf_size=pdf_cache_key["pdf_size"],
                        pdf_mtime_ns=pdf_cache_key["pdf_mtime_ns"],
                        normalized_text_sha256=normalized_hash,
                        normalized_len=normalized_len,
                        direct_len=direct_len,
                        ratio=ratio,
                        parser_level=str(parser_level or "").strip(),
                        checked_at=datetime.now(timezone.utc).isoformat(),
                    ),
                )
            return

        normalized_direct = normalize_extracted_text_for_llm(direct_text).strip()
        if normalized_direct:
            if self._compact_artifacts_enabled():
                self._persist_compact_text_artifacts(
                    paper,
                    pdf_path,
                    pdf_cache_key,
                    normalized_direct,
                    [],
                    parser_level=parser_level,
                )
            else:
                metadata_snapshot = self._metadata_snapshot_for_folder(folder_path, fallback=paper.metadata)
                self._write_compact_human_normalized_text(
                    folder_path,
                    metadata_snapshot,
                    normalized_direct,
                    target_stage="full_text",
                    paper_id=str(paper.paper_id),
                )
                artifact_path = self._compact_artifact_path_for_folder(
                    folder_path,
                    stage="full_text",
                    paper_id=str(paper.paper_id),
                )
                if artifact_path.exists():
                    try:
                        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
                        if isinstance(payload, dict):
                            payload["normalized_text"] = normalized_direct
                            payload["normalized_text_sha256"] = self._sha256_text(normalized_direct)
                            payload["parser_level"] = str(parser_level or payload.get("parser_level") or "").strip()
                            payload["updated_at"] = datetime.now(timezone.utc).isoformat()
                            artifact_path.write_text(
                                json.dumps(payload, ensure_ascii=False, indent=2),
                                encoding="utf-8",
                            )
                    except Exception as exc:
                        self._log_error(
                            paper.paper_id,
                            f"failed to update compact artifact after normalized-text refresh: {exc}",
                            error_type="compact_artifact_update_failed",
                        )

        refreshed_len = len((normalized_direct or "").strip())
        refreshed_ratio = refreshed_len / float(direct_len) if direct_len else 0.0
        if pdf_cache_key and (normalized_direct or normalized_value):
            # human readable hint: if the artifact rewrite touched the PDF sidecar, reread the stat key before caching.
            try:
                refreshed_pdf_cache_key = self._pdf_ratio_cache_key(pdf_path)
            except Exception:
                refreshed_pdf_cache_key = pdf_cache_key
            refreshed_hash = self._sha256_text(normalized_direct) if normalized_direct else normalized_hash
            refreshed_cache_len = refreshed_len if normalized_direct else normalized_len
            refreshed_cache_ratio = refreshed_ratio if normalized_direct else ratio
            refreshed_artifact_path, refreshed_payload, _cached = self._full_text_ratio_artifact_payload(
                folder_path,
                paper,
            )
            self._write_full_text_ratio_check_cache(
                artifact_path=refreshed_artifact_path,
                payload=refreshed_payload,
                paper=paper,
                folder_path=folder_path,
                cache_entry=FullTextRatioCheckCache(
                    pdf_size=refreshed_pdf_cache_key["pdf_size"],
                    pdf_mtime_ns=refreshed_pdf_cache_key["pdf_mtime_ns"],
                    normalized_text_sha256=refreshed_hash,
                    normalized_len=refreshed_cache_len,
                    direct_len=direct_len,
                    ratio=refreshed_cache_ratio,
                    parser_level=str(parser_level or "").strip(),
                    checked_at=datetime.now(timezone.utc).isoformat(),
                ),
            )
        if refreshed_ratio < min_ratio and not self.quiet:
            print(
                "[warning] full_text sanity check ratio below minimum after parser chain "
                f"for paper={paper.paper_id} ratio={refreshed_ratio:.2f} "
                f"normalized_len={refreshed_len} direct_len={direct_len} parser='{parser_level}'"
            )

    def _materialize_paper_folders_full_text(self) -> None:
        """Split select CSV rows into per-paper folders under the configured full-text PDF folder."""

        csv_rows = self._collect_csv_rows(select_only=True)
        if not csv_rows:
            print("[warning] no select CSV rows found for materialization")
            return

        base_dir = self._full_text_pdf_dir()
        base_dir.mkdir(parents=True, exist_ok=True)
        folders: list[Path] = []

        for row in csv_rows:
            folder_name = self._build_paper_folder_name(row)
            folder_path = base_dir / folder_name
            folder_path.mkdir(parents=True, exist_ok=True)

            canonical = self._canonicalize_row(row)

            artifact_path = self._compact_artifact_path_for_folder(folder_path, stage="full_text")
            artifact_payload: dict[str, Any] = {}
            if artifact_path.exists():
                try:
                    loaded = json.loads(artifact_path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        artifact_payload = loaded
                except Exception:
                    artifact_payload = {}

            resolved_parser_level = str(artifact_payload.get("parser_level") or "").strip()
            payload_without_level = {k: v for k, v in artifact_payload.items() if k != "parser_level"}
            artifact_payload = {"parser_level": resolved_parser_level}
            artifact_payload.update(payload_without_level)
            artifact_payload.update(
                {
                    "meta": "stage_artifact",
                    "schema_version": 1,
                    "stage": "full_text",
                    "paper_id": str(canonical.get("paper_id") or folder_name),
                    "metadata": canonical,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            artifact_path.write_text(
                json.dumps(artifact_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            for stale_name in ("metadata.json", "metadata.csv"):
                stale_path = folder_path / stale_name
                if stale_path.exists():
                    try:
                        stale_path.unlink()
                    except Exception:
                        pass

            if not any(folder_path.glob("*.pdf")):
                paper_id = str(canonical.get("paper_id") or "").strip()
                source_pdf = self._find_source_pdf_for_paper_id(paper_id)
                if source_pdf and source_pdf.exists():
                    target_pdf = folder_path / PerPaperFileIndex.canonical_pdf_filename(paper_id or folder_name)
                    try:
                        if source_pdf.resolve() != target_pdf.resolve():
                            shutil.copy2(source_pdf, target_pdf)
                    except Exception as exc:  # pylint: disable=broad-except
                        self._log_error(
                            paper_id or folder_name,
                            f"failed to copy source PDF into retry folder: {exc}",
                            error_type="retry_pdf_copy_failed",
                        )
                elif self.pdf_root and self.pdf_root.exists():
                    self._log_error(
                        paper_id or folder_name,
                        f"no source PDF found in pdf_root={self.pdf_root} for retry folder materialization",
                        error_type="retry_pdf_source_missing",
                    )

            folders.append(folder_path)

        self._paper_folders = folders

