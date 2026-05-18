from __future__ import annotations
from pipeline.core import pipeline as _pipeline

globals().update({name: getattr(_pipeline, name) for name in dir(_pipeline) if not name.startswith("__")})

class DataExtractionStageMixin:
    def _ensure_full_text_normalized_for_data_extraction(self, paper: PaperRecord) -> bool:
        """human readable hint: run the full-text PDF parsing/cache path inside data_extraction when evidence is missing."""

        folder = paper.metadata.get("folder_path")
        if not folder:
            self._log_error(
                paper.paper_id,
                "data_extraction preflight cannot find paper folder metadata",
                error_type="data_extraction_preflight_folder_missing",
            )
            return False

        folder_path = Path(folder)
        if self._has_full_text_normalized_content(folder_path):
            resolved_path = self._resolve_pdf_path(paper)
            if resolved_path and resolved_path.exists():
                self._refresh_full_text_normalized_if_ratio_low(paper, folder_path, resolved_path)
            return True

        resolved_path = self._resolve_pdf_path(paper)
        if not resolved_path or not resolved_path.exists():
            return False

        text, _page_count, used_path, _pages = self._load_pdf_text(
            paper,
            resolved_path,
            include_pages=False,
        )
        ok = bool(normalize_extracted_text_for_llm(text or "").strip()) and self._has_full_text_normalized_content(
            folder_path
        )
        if ok and used_path:
            self._refresh_full_text_normalized_if_ratio_low(paper, folder_path, used_path)
        return ok

    def _preflight_data_extraction_full_text_inputs(self, papers: list[PaperRecord]) -> None:
        """human readable hint: prepare only the active extraction PDFs after QC filtering."""

        if not self._data_extraction_preflight_enabled or not papers:
            return

        self._suppress_noisy_parser_library_logs()

        total = len(papers)
        already_ready = 0
        generated = 0
        chunks_ready = 0
        chunks_generated = 0
        failed = 0
        report_rows: list[dict[str, str]] = []

        if not self.quiet:
            print(f"[preflight] Checking data_extraction normalized full text for {total} paper(s)...")

        for idx, paper in enumerate(papers, start=1):
            folder = paper.metadata.get("folder_path")
            folder_path = Path(folder) if folder else None
            was_ready = bool(folder_path and self._has_full_text_normalized_content(folder_path))
            ok = self._ensure_full_text_normalized_for_data_extraction(paper)
            had_chunks = self._has_data_extraction_selected_chunks(paper)

            if ok and was_ready:
                already_ready += 1
                status = "READY"
            elif ok:
                generated += 1
                status = "GENERATED"
            else:
                failed += 1
                status = "FAIL"

            parser_level = self._read_parser_level_for_folder(folder_path) if folder_path else ""
            parse_status = "OK" if ok else "FAIL"

            # human readable hint: selected chunks are audit evidence; full normalized text remains the default LLM input.
            chunks_status = "SKIPPED"
            if ok:
                if had_chunks:
                    chunks_ready += 1
                    chunks_status = "READY"
                elif self._ensure_data_extraction_selected_chunks(paper):
                    chunks_generated += 1
                    chunks_status = "GENERATED"
                else:
                    chunks_status = "FAIL"

            report_rows.append(
                {
                    "paper_id": str(paper.paper_id),
                    "status": status,
                    "parse_status": parse_status,
                    "parser_level": parser_level,
                    "selected_chunks_status": chunks_status,
                    "folder_path": str(folder_path or ""),
                }
            )

            if not self.quiet and self._fulltext_preparse_log_each_paper:
                parser_suffix = f" parser='{parser_level}'" if parser_level else ""
                print(
                    f"[preflight] {idx}/{total} paper={paper.paper_id} "
                    f"normalized_text={status} status={parse_status}{parser_suffix}",
                    flush=True,
                )

        self._write_data_extraction_preflight_report(
            report_rows,
            total,
            already_ready,
            generated,
            chunks_ready,
            chunks_generated,
            failed,
        )

        if not self.quiet:
            print(
                "[preflight] Data extraction full-text evidence ready: "
                f"ready={already_ready} generated={generated} "
                f"chunks_ready={chunks_ready} chunks_generated={chunks_generated} fail={failed} total={total}",
                flush=True,
            )

    def _ensure_data_extraction_selected_chunks(self, paper: PaperRecord) -> bool:
        """human readable hint: create data_extraction chunk-audit sidecars before the LLM run."""

        try:
            chunks, _pdf_text_tokens, _pdf_visual_tokens, _language_used = self._prepare_chunks(paper)
            dropped_low_quality_candidates: list[dict] = []
            if chunks:
                chunks, _dropped_count, dropped_low_quality_candidates = self._filter_low_quality_chunks(chunks)
            if not chunks:
                return False
            selected, _embed_usage, _selection_trace = self._select_chunks_with_rescue(
                chunks,
                dropped_low_quality_candidates,
            )
            selected, _score_stats = self._attach_chunk_certainty_metrics(selected)
            self._write_selected_chunks_to_input(paper, selected)
            return bool(selected)
        except Exception as exc:  # pylint: disable=broad-except
            self._log_error(
                paper.paper_id,
                f"data extraction selected-chunk preflight failed: {exc}",
                error_type="data_extraction_selected_chunks_preflight_failed",
            )
            return False

    def _has_data_extraction_selected_chunks(self, paper: PaperRecord) -> bool:
        """human readable hint: check only data_extraction chunk sidecars, not older full_text fallbacks."""

        folder = paper.metadata.get("folder_path")
        if not folder:
            return False

        folder_path = Path(folder)
        chunks_path = self._first_existing_prefixed_path(
            folder_path,
            "data_extraction_selected_chunks.jsonl",
            paper_id=str(paper.paper_id),
        )
        if chunks_path:
            try:
                with chunks_path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if not line.strip():
                            continue
                        payload = json.loads(line)
                        if payload.get("paper_id") == paper.paper_id and isinstance(payload.get("selected_chunks"), list):
                            return bool(payload.get("selected_chunks"))
            except Exception:
                return False

        artifact_path = self._first_existing_prefixed_path(
            folder_path,
            "data_extraction_artifact.json",
            paper_id=str(paper.paper_id),
        )
        if artifact_path:
            try:
                payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            except Exception:
                return False
            if not isinstance(payload, dict):
                return False
            return bool(payload.get("selected_chunks")) if isinstance(payload.get("selected_chunks"), list) else False
        return False

    def _write_data_extraction_preflight_report(
        self,
        rows: list[dict[str, str]],
        total: int,
        already_ready: int,
        generated: int,
        chunks_ready: int,
        chunks_generated: int,
        failed: int,
    ) -> None:
        """human readable hint: record which data_extraction folders needed normalized text generation."""

        payload = {
            "meta": "data_extraction_full_text_preflight_report",
            "stage": self.stage,
            "run_label": self.run_label,
            "run_id": self.run_id,
            "total": total,
            "already_ready": already_ready,
            "generated": generated,
            "selected_chunks_ready": chunks_ready,
            "selected_chunks_generated": chunks_generated,
            "failed": failed,
            "rows": rows,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self.stage_output_dir.mkdir(parents=True, exist_ok=True)
            report_path = self.stage_output_dir / f"{self.stage}_full_text_preflight_report.json"
            # human readable hint: keep one latest preflight report so repeated QC runs do not clutter the output folder.
            report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:  # pylint: disable=broad-except
            if not self.quiet:
                print(f"[warning] Could not write data_extraction preflight report: {exc}")

    def _load_data_extraction_full_text_input(self, paper: PaperRecord) -> str:
        """human readable hint: use the cached normalized full text as extraction evidence when available."""

        folder = paper.metadata.get("folder_path")
        if not folder:
            return ""

        folder_path = Path(folder)
        candidates: list[Path] = []
        for name in ("full_text_normalized.txt", "data_extraction_normalized.txt"):
            candidate = self._first_existing_prefixed_path(
                folder_path,
                name,
                paper_id=str(paper.paper_id),
            )
            if candidate:
                candidates.append(candidate)
        raw_text = ""
        normalized_text = ""
        for candidate in candidates:
            try:
                raw_text = candidate.read_text(encoding="utf-8")
                break
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    paper.paper_id,
                    f"failed to read normalized full text for extraction: {exc}",
                    error_type="data_extraction_full_text_read_failed",
                )
                return ""

        if raw_text:
            marker = "=== normalized_full_text ==="
            marker_index = raw_text.find(marker)
            if marker_index >= 0:
                raw_text = raw_text[marker_index + len(marker):]
            # human readable hint: preserve table structure while cleaning PDF artifacts for extraction.
            normalized_text = normalize_extracted_text_for_llm(raw_text).strip()

        if not normalized_text and bool(LLM_SETTINGS.get("data_extraction_generate_normalized_text", False)):
            # human readable hint: reuse the same normalized full-text sidecar that full_text screening writes.
            if self._ensure_full_text_normalized_for_data_extraction(paper):
                try:
                    candidate = self._first_existing_prefixed_path(
                        folder_path,
                        "full_text_normalized.txt",
                        paper_id=str(paper.paper_id),
                    )
                    if candidate:
                        raw_text = candidate.read_text(encoding="utf-8")
                    marker = "=== normalized_full_text ==="
                    marker_index = raw_text.find(marker)
                    if marker_index >= 0:
                        raw_text = raw_text[marker_index + len(marker):]
                    normalized_text = normalize_extracted_text_for_llm(raw_text).strip()
                except Exception as exc:  # pylint: disable=broad-except
                    self._log_error(
                        paper.paper_id,
                        f"failed to read generated normalized full text for extraction: {exc}",
                        error_type="data_extraction_generated_full_text_read_failed",
                    )
        if not normalized_text:
            return ""

        max_words = int(LLM_SETTINGS.get("data_extraction_full_text_max_words", 0) or 0)
        if max_words > 0:
            words = normalized_text.split()
            if len(words) > max_words:
                normalized_text = " ".join(words[:max_words])

        title_text = (paper.title or "").strip()
        parts = [f"Paper ID: {paper.paper_id}"]
        if title_text:
            parts.append(f"Title: {title_text}")
        evidence_hints = self._build_data_extraction_schema_evidence_hints(normalized_text)
        if evidence_hints:
            parts.append(evidence_hints)
        parts.append("[Full Normalized Text]\n" + normalized_text)
        supplemental_cited_evidence = SupplementalCitedEvidenceLoader.from_user_config().load_for_folder(folder_path)
        if supplemental_cited_evidence:
            parts.append(supplemental_cited_evidence)
        return "\n\n".join(parts)

    def _build_data_extraction_schema_evidence_hints(self, normalized_text: str) -> str:
        """human readable hint: prepend compact schema-derived snippets so extraction does not miss buried evidence."""

        return self._build_schema_evidence_hints_for_schema(normalized_text, self._extraction_schema)

    @staticmethod
    def _data_extraction_schema_evidence_hint_config() -> SchemaEvidenceHintConfig:
        """human readable hint: centralize evidence-hint limits so full and domain-scoped prompts stay aligned."""

        return SchemaEvidenceHintConfig(
            enabled=True,
            snippets_per_variable=max(0, int(LLM_SETTINGS.get("data_extraction_evidence_hints_per_variable", 2) or 0)),
            max_snippet_chars=max(120, int(LLM_SETTINGS.get("data_extraction_evidence_hint_max_chars", 420) or 420)),
            max_total_chars=max(1000, int(LLM_SETTINGS.get("data_extraction_evidence_hints_max_total_chars", 18000) or 18000)),
            context_lines=max(0, int(LLM_SETTINGS.get("data_extraction_evidence_hint_context_lines", 1) or 0)),
            alias_map=DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_ALIASES,
            low_priority_patterns=tuple(DATA_EXTRACTION_SCHEMA_EVIDENCE_HINT_LOW_PRIORITY_PATTERNS),
        )

    @classmethod
    def _build_schema_evidence_hints_for_schema(
        cls,
        normalized_text: str,
        schema: DynamicExtractionSchema | None,
    ) -> str:
        """human readable hint: build evidence hints for either the full schema or the active domain group."""

        if not bool(LLM_SETTINGS.get("data_extraction_schema_evidence_hints", True)):
            return ""
        if schema is None:
            return ""
        config = cls._data_extraction_schema_evidence_hint_config()
        # human readable hint: the builder lives with the schema so runtime prompts and trace audits share the same evidence map.
        return SchemaEvidenceHintBuilder(schema.variables, config).build(normalized_text)

    def _with_domain_scoped_schema_evidence_hints(
        self,
        context: str,
        domain_schema: DynamicExtractionSchema,
    ) -> str:
        """human readable hint: reduce repeated extraction calls by sending only hints for the active domain group."""

        if not bool(LLM_SETTINGS.get("data_extraction_schema_evidence_hints", True)):
            return context

        full_text_marker = "[Full Normalized Text]\n"
        marker_index = context.find(full_text_marker)
        if marker_index < 0:
            return context

        prefix = context[:marker_index].rstrip()
        normalized_text = context[marker_index + len(full_text_marker) :].strip()
        if not normalized_text:
            return context

        hint_marker = "[Schema-Guided Evidence Hints]"
        hint_index = prefix.find(hint_marker)
        if hint_index >= 0:
            prefix = prefix[:hint_index].rstrip()

        evidence_hints = self._build_schema_evidence_hints_for_schema(normalized_text, domain_schema)
        parts = [prefix] if prefix else []
        if evidence_hints:
            parts.append(evidence_hints)
        parts.append(full_text_marker + normalized_text)
        return "\n\n".join(parts)

    def _find_source_pdf_for_paper_id(self, paper_id: str) -> Path | None:
        """Locate source PDF in pdf_root using the configured paper ID for retry materialization."""

        if not self.pdf_root or not self.pdf_root.exists():
            return None

        cid = str(paper_id or "").strip().lstrip("#")
        if not cid:
            return None

        safe_id = self._sanitize_paper_id_for_filename(cid)
        artifact_prefix = f"{safe_id}_"
        legacy_prefix = f"#{safe_id}_"

        candidate_folders: list[Path] = []
        direct = self.pdf_root / cid
        if direct.is_dir():
            candidate_folders.append(direct)

        try:
            prefixed = sorted(
                [
                    folder
                    for folder in self.pdf_root.iterdir()
                    if folder.is_dir() and folder.name.startswith(f"{cid}_")
                ],
                key=lambda folder: folder.stat().st_mtime,
                reverse=True,
            )
            candidate_folders.extend(prefixed)
        except Exception:
            pass

        unique_folders: list[Path] = []
        seen: set[str] = set()
        for folder in candidate_folders:
            key = str(folder.resolve())
            if key in seen:
                continue
            seen.add(key)
            unique_folders.append(folder)

        for folder in unique_folders:
            preferred = folder / f"{cid}.pdf"
            if preferred.exists():
                return preferred
            canonical_id = folder / PerPaperFileIndex.canonical_pdf_filename(safe_id)
            if canonical_id.exists():
                return canonical_id
            prefixed_pdfs = sorted(folder.glob(f"{artifact_prefix}*.pdf"))
            if prefixed_pdfs:
                return prefixed_pdfs[0]
            legacy_prefixed_pdfs = sorted(folder.glob(f"{legacy_prefix}*.pdf"))
            if legacy_prefixed_pdfs:
                return legacy_prefixed_pdfs[0]
            canonical = folder / PAPER_PDF_NAME
            if canonical.exists():
                return canonical
            pdfs = sorted(folder.glob("*.pdf"))
            if pdfs:
                return pdfs[0]

        return None

    def _materialize_data_extraction_subset(self) -> None:
        """Create per-paper data_extraction folders from included IDs."""
        # human readable hint: use the active stage CSV(s) so every included ID gets a folder.
        self._materialize_data_extraction_from_csv_inputs()

    def _materialize_data_extraction_from_csv_inputs(self) -> None:
        """Create per-paper data_extraction folders from CSV inputs or stage defaults."""

        csv_rows = self._collect_csv_rows(select_only=False)
        if not csv_rows:
            print("[warning] no input CSV rows found; data_extraction subset not prepared")
            self._paper_folders = []
            return

        target_dir = self._data_extraction_pdf_dir()
        target_dir.mkdir(parents=True, exist_ok=True)

        # human readable hint: reuse existing full_text artifacts when available to avoid re-parsing PDFs.
        source_dir = self._full_text_pdf_dir()
        source_lookup: dict[str, Path] = {}
        if source_dir.exists():
            for folder in sorted(source_dir.iterdir()):
                if not folder.is_dir():
                    continue
                row = self._metadata_snapshot_for_folder(folder)
                if not row:
                    continue
                paper_id = self._extract_paper_id(row)
                if paper_id:
                    source_lookup[str(paper_id)] = folder

        copied: list[Path] = []
        missing: list[str] = []
        seen_ids: set[str] = set()

        for row in csv_rows:
            canonical = self._canonicalize_row(row)
            paper_id = str(read_metadata_value(canonical, "paper_id", "")).strip()
            folder_name = self._build_paper_folder_name(row)
            unique_key = paper_id or folder_name
            if unique_key in seen_ids:
                continue
            seen_ids.add(unique_key)

            dest = target_dir / folder_name
            dest.mkdir(parents=True, exist_ok=True)

            data_artifact_path = self._compact_artifact_path_for_folder(
                dest,
                stage="data_extraction",
                paper_id=paper_id or folder_name,
            )
            data_artifact_payload: dict[str, Any] = {}
            if data_artifact_path.exists():
                try:
                    loaded = json.loads(data_artifact_path.read_text(encoding="utf-8"))
                    if isinstance(loaded, dict):
                        data_artifact_payload = loaded
                except Exception:
                    data_artifact_payload = {}

            data_artifact_payload.update(
                {
                    "meta": "stage_artifact",
                    "schema_version": 1,
                    "stage": "data_extraction",
                    "paper_id": paper_id or folder_name,
                    "metadata": canonical,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            data_artifact_path.write_text(
                json.dumps(data_artifact_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            source_folder = source_lookup.get(paper_id) if paper_id else None
            if source_folder and source_folder.exists():
                self._copy_reusable_full_text_artifacts(source_folder, dest)
                if not any(dest.glob("*.pdf")):
                    pdfs = sorted(source_folder.glob("*.pdf"))
                    if pdfs:
                        target_pdf = dest / PerPaperFileIndex.canonical_pdf_filename(paper_id or folder_name)
                        shutil.copy2(pdfs[0], target_pdf)

            if not any(dest.glob("*.pdf")) and paper_id:
                source_pdf = self._find_source_pdf_for_paper_id(paper_id)
                if source_pdf and source_pdf.exists():
                    target_pdf = dest / PerPaperFileIndex.canonical_pdf_filename(paper_id)
                    shutil.copy2(source_pdf, target_pdf)

            if not any(dest.glob("*.pdf")):
                missing.append(dest.name)

            copied.append(dest)

        self._paper_folders = copied

        # human readable hint: stop early when PDFs are missing so extraction does not silently skip evidence.
        if missing and not self.split_only:
            print(
                f"[prep] PDFs missing for {len(missing)} folder(s) in {target_dir.name}. "
                "Add one PDF per folder, then rerun main.py:"
            )
            for name in missing:
                print(f"  - {name}")
            self._paper_folders = []
            return

    def _copy_reusable_full_text_artifacts(self, source_folder: Path, dest_folder: Path) -> None:
        """human readable hint: carry full-text parsing results forward instead of deleting or re-parsing them."""

        paper_id = self._infer_paper_id_from_folder(dest_folder)
        prefix = f"{paper_id}_"
        legacy_prefix = f"#{paper_id}_"

        core_names = ["full_text_artifact.json", "full_text_normalized.txt"]
        for name in core_names:
            dest_path = self._prefixed_path_candidates(dest_folder, name, paper_id=paper_id)[0]
            if dest_path.exists():
                continue
            source_candidates = self._prefixed_path_candidates(source_folder, name, paper_id=paper_id)
            source_path = next((candidate for candidate in source_candidates if candidate.exists()), None)
            if not source_path:
                continue
            try:
                shutil.copy2(source_path, dest_path)
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    dest_folder.name,
                    f"failed to copy reusable full-text artifact '{name}': {exc}",
                    error_type="full_text_artifact_copy_failed",
                )

        cache_patterns = ["*_normalized_text.txt", "*_normalized_pages.json", "*_normalized_meta.json"]
        for pattern in cache_patterns:
            for source_path in sorted(source_folder.glob(pattern)):
                if not (source_path.name.startswith(prefix) or source_path.name.startswith(legacy_prefix)):
                    continue
                dest_name = source_path.name
                if dest_name.startswith(legacy_prefix):
                    dest_name = f"{prefix}{dest_name[len(legacy_prefix):]}"
                dest_path = dest_folder / dest_name
                if dest_path.exists():
                    continue
                try:
                    shutil.copy2(source_path, dest_path)
                except Exception as exc:  # pylint: disable=broad-except
                    self._log_error(
                        dest_folder.name,
                        f"failed to copy reusable full-text artifact '{source_path.name}': {exc}",
                        error_type="full_text_artifact_copy_failed",
                    )

    async def _maybe_run_data_extraction_hybrid_rescue_async(
        self,
        *,
        paper: PaperRecord,
        extraction_payload: dict[str, Any],
        primary_context: str,
    ) -> None:
        """human readable hint: run optional semantic rescue after primary full-text extraction."""

        if (
            self._extraction_schema is None
            or self._hybrid_rescue_config is None
            or not self._hybrid_rescue_config.enabled
            or self._hybrid_rescue_planner is None
            or self._hybrid_rescue_evidence_builder is None
            or self._hybrid_rescue_selector is None
        ):
            return

        primary_payload = extraction_payload.get("extracted_data")
        if not isinstance(primary_payload, dict):
            return

        target_variables = self._hybrid_rescue_planner.target_variables(primary_payload)
        if not target_variables:
            return

        decisions = []
        variables_by_domain: dict[str, list] = {}
        for variable in target_variables:
            variables_by_domain.setdefault(variable.domain, []).append(variable)

        response_format_mode = str(
            LLM_SETTINGS.get("data_extraction_response_format_mode", "prompt_only") or "prompt_only"
        ).strip().lower()
        domain_max_tokens = max(256, int(LLM_SETTINGS.get("data_extraction_domain_max_tokens", 3000) or 3000))

        # human readable hint: rescue calls stay domain-scoped so each second opinion is small and auditable.
        for domain, variables in variables_by_domain.items():
            try:
                rescue_context = await asyncio.to_thread(
                    self._hybrid_rescue_evidence_builder.build_context,
                    paper_id=paper.paper_id,
                    title=paper.title,
                    primary_context=primary_context,
                    variables=tuple(variables),
                )
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    paper.paper_id,
                    f"data extraction hybrid rescue evidence build failed for domain={domain}: {exc}",
                    error_type="data_extraction_hybrid_rescue_evidence_failed",
                )
                continue

            if not rescue_context:
                continue

            domain_schema = self._extraction_schema.for_domains((domain,))
            domain_prompt = domain_schema.inject_into_prompt(self._base_prompt_template)
            response_format_override: dict | None = None
            use_schema_response_format = response_format_mode == "json_schema"
            if response_format_mode == "json_object":
                response_format_override = {"type": "json_object"}

            raw_text, _usage = await self._call_llm_async(
                rescue_context,
                prompt_template=domain_prompt,
                extraction_schema=domain_schema,
                response_format_override=response_format_override,
                use_extraction_response_format=use_schema_response_format,
                max_tokens=domain_max_tokens,
                system_prompt="",
            )
            rescue_payload, validation_error = validate_llm_extraction(raw_text, domain_schema)
            if validation_error:
                failed_call = not raw_text or (isinstance(raw_text, str) and raw_text.startswith("LLM error"))
                self._log_error(
                    paper.paper_id,
                    (
                        f"data extraction hybrid rescue LLM failed for domain={domain}: {validation_error}"
                        if failed_call
                        else f"data extraction hybrid rescue validation failed for domain={domain}: {validation_error}"
                    ),
                    error_type=(
                        "data_extraction_hybrid_rescue_llm_failed"
                        if failed_call
                        else "data_extraction_hybrid_rescue_validation_failed"
                    ),
                )
                continue

            for variable in variables:
                decisions.append(
                    self._hybrid_rescue_selector.decide(
                        paper_id=paper.paper_id,
                        variable=variable,
                        primary_payload=primary_payload,
                        rescue_payload=rescue_payload,
                    )
                )

        if not decisions:
            return

        extraction_payload["hybrid_rescue"] = [
            {
                "paper_id": decision.paper_id,
                "variable": decision.variable,
                "primary_full_text_value": decision.primary_full_text_value,
                "primary_full_text_quote": decision.primary_full_text_quote,
                "semantic_rescue_value": decision.semantic_rescue_value,
                "semantic_rescue_quote": decision.semantic_rescue_quote,
                "selected_value": decision.selected_value,
                "selected_quote": decision.selected_quote,
                "evidence_mode_used": decision.evidence_mode_used,
                "selection_reason": decision.selection_reason,
            }
            for decision in decisions
        ]
        if self._hybrid_rescue_writer is not None:
            await asyncio.to_thread(self._hybrid_rescue_writer.add_decisions, decisions)

    async def _call_data_extraction_domains_async(
        self,
        context: str,
        paper_id: str,
    ) -> tuple[str | None, dict | None, dict[str, str]]:
        """human readable hint: extract each KB domain separately, then merge validated domain JSON."""

        if self._extraction_schema is None:
            return "LLM error: data_extraction requires a configured DynamicExtractionSchema.", None, {
                "schema": "missing_extraction_schema"
            }

        if not bool(LLM_SETTINGS.get("data_extraction_split_by_domain", True)):
            raw_text, usage = await self._call_llm_async(context)
            return raw_text, usage, {}

        merged_payload = self._extraction_schema.default_payload()
        errors_by_domain: dict[str, str] = {}
        usage_totals: dict[str, int] = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "response_tokens": 0,
            "total_tokens": 0,
        }
        domain_max_tokens = max(256, int(LLM_SETTINGS.get("data_extraction_domain_max_tokens", 3000) or 3000))
        response_format_mode = str(
            LLM_SETTINGS.get("data_extraction_response_format_mode", "prompt_only") or "prompt_only"
        ).strip().lower()

        domain_groups = domain_groups_for_schema(
            self._extraction_schema,
            LLM_SETTINGS.get("data_extraction_domain_groups"),
        )
        for domains in domain_groups:
            group_label = "+".join(domains)
            domain_schema = self._extraction_schema.for_domains(domains)
            domain_prompt = domain_schema.inject_into_prompt(self._base_prompt_template)
            response_format_override: dict | None = None
            use_schema_response_format = response_format_mode == "json_schema"
            if response_format_mode == "json_object":
                response_format_override = {"type": "json_object"}
            domain_context = self._with_domain_scoped_schema_evidence_hints(context, domain_schema)
            raw_text, usage = await self._call_llm_async(
                domain_context,
                prompt_template=domain_prompt,
                extraction_schema=domain_schema,
                response_format_override=response_format_override,
                use_extraction_response_format=use_schema_response_format,
                max_tokens=domain_max_tokens,
                system_prompt="",
            )
            self._add_llm_usage(usage_totals, usage)
            parsed_domain, validation_error = validate_llm_extraction(raw_text, domain_schema)
            if validation_error:
                errors_by_domain[group_label] = validation_error
                continue

            for domain in domains:
                domain_payload = parsed_domain.get(domain)
                if isinstance(domain_payload, dict):
                    merged_payload[domain] = domain_payload
                else:
                    errors_by_domain[domain] = "validated domain payload missing expected domain key"

        try:
            merged_payload = self._extraction_schema.validate_payload(merged_payload)
        except Exception as exc:  # pylint: disable=broad-except
            errors_by_domain["merged_payload"] = str(exc)
            merged_payload = self._extraction_schema.default_payload()

        usage_totals["domain_count"] = len(self._extraction_schema.domains)
        usage_totals["domain_group_count"] = len(domain_groups)
        usage_totals["domain_error_count"] = len(errors_by_domain)
        return json.dumps(merged_payload, ensure_ascii=False), usage_totals, errors_by_domain

    def _write_data_extraction_metadata(
        self,
        paper: PaperRecord,
        selected: list[dict],
        decision: str | None,
        extraction_payload: dict | None,
    ) -> None:
        """Write evidence.json linking extracted fields to selected chunks."""

        output_dir = self._data_extraction_output_dir(paper)
        output_dir.mkdir(parents=True, exist_ok=True)
        meta_path = output_dir / f"{self.stage}_evidence.json"

        payload = {
            "paper_id": paper.paper_id,
            "run_label": self.run_label,
            "run_id": self.run_id,
            "llm_decision": decision,
            "selected_chunks": selected,
            "extracted_data": extraction_payload.get("extracted_data") if extraction_payload else None,
            "extracted_data_flat": extraction_payload.get("extracted_data_flat") if extraction_payload else None,
            "field_provenance": extraction_payload.get("field_provenance") if extraction_payload else None,
            "criteria": self._extraction_criteria,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            with open(meta_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
        except Exception as exc:  # pylint: disable=broad-except
            self._log_error(paper.paper_id, f"failed to write evidence metadata: {exc}", error_type="evidence_write_failed")

    def _data_extraction_output_dir(self, paper: PaperRecord) -> Path:
        """Return the output folder for this paper in data_extraction."""

        folder = paper.metadata.get("folder_path")
        if folder:
            return self.stage_output_dir / Path(folder).name
        return self.stage_output_dir / str(paper.paper_id)

    def _write_data_extraction_outputs(self, paper: PaperRecord, extraction_payload: dict) -> None:
        """human readable hint: delegate per-paper extraction artifact I/O to extraction_io."""

        output_dir = self._data_extraction_output_dir(paper)
        write_outputs(
            extraction_payload,
            output_dir.parent,
            output_dir.name,
            stage=self.stage,
            run_label=self.run_label,
            criteria=self._extraction_criteria,
        )

        if self._extraction_aggregate_writer is not None:
            # human readable hint: update run-level comparison and quote-audit CSVs as soon as this paper finishes.
            try:
                self._extraction_aggregate_writer.append_record(extraction_payload)
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    paper.paper_id,
                    f"data extraction aggregate table append failed: {exc}",
                    error_type="data_extraction_aggregate_append_failed",
                )

    def _start_data_extraction_aggregate_writer(self) -> None:
        """human readable hint: create live aggregate extraction CSVs before the first paper is screened."""

        if self._hybrid_rescue_writer is not None:
            # human readable hint: hybrid audit files mirror the live aggregate tables and reset for QC-only runs.
            try:
                if bool(self.qc_only):
                    self._hybrid_rescue_writer.reset()
                else:
                    self._hybrid_rescue_writer.write()
            except Exception as exc:  # pylint: disable=broad-except
                self._log_error(
                    "run",
                    f"data extraction hybrid rescue audit initialization failed: {exc}",
                    error_type="data_extraction_hybrid_rescue_audit_init_failed",
                )

        try:
            self._extraction_aggregate_writer = ExtractionAggregateWriter(
                output_dir=self.stage_output_dir,
                consensus_path=self.csv_dir / "data_extraction_schema.csv",
                input_paper_dir=self._data_extraction_pdf_dir(),
                reset=bool(self.qc_only),
            )
            if not self.quiet:
                print(
                    "[extraction] Aggregate tables ready: "
                    f"{self._extraction_aggregate_writer.comparison_path.name}, "
                    f"{self._extraction_aggregate_writer.quote_path.name}"
                )
        except Exception as exc:  # pylint: disable=broad-except
            self._extraction_aggregate_writer = None
            self._log_error(
                "run",
                f"data extraction aggregate table initialization failed: {exc}",
                error_type="data_extraction_aggregate_init_failed",
            )

    def _build_extraction_payload(self, paper: PaperRecord, llm_decision: str | None) -> dict | None:
        """Parse the LLM output into structured extraction data."""

        if not llm_decision:
            return None

        if self._extraction_schema is None:
            raise RuntimeError("data_extraction requires a configured DynamicExtractionSchema.")

        # human readable hint: validate LLM JSON against the same KB-generated Pydantic model sent to OpenAI.
        filtered, validation_error = validate_llm_extraction(llm_decision, self._extraction_schema)
        if validation_error:
            self._log_error(
                paper.paper_id,
                f"data extraction schema validation failed: {validation_error}",
                error_type="data_extraction_schema_validation_failed",
            )
        return {
            "paper_id": paper.paper_id,
            "run_label": self.run_label,
            "run_id": self.run_id,
            "extracted_data": filtered,
            "extracted_data_flat": flatten_extracted_data(filtered),
            "field_provenance": {},
            "raw_output": llm_decision,
            "schema_kb_path": str(self._extraction_schema.kb_path),
            "schema_validation_error": validation_error,
        }

