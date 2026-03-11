"""Resource usage tracking and CodeCarbon integration.

This module records token counts and CodeCarbon totals, then derives per-token
rates from CodeCarbon outputs for the screening pipeline.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, Optional

from config.user_orchestrator import CARBON_CONFIG, HUMAN_TIME_CONFIG, UBELIX_ESTIMATION_CONFIG


def _estimate_ubelix_operational(total_runtime_seconds: float) -> dict[str, Any] | None:
	"""Estimate UBELIX operational energy/CO2e with Green-Algorithms style factors."""

	config = UBELIX_ESTIMATION_CONFIG if isinstance(UBELIX_ESTIMATION_CONFIG, dict) else {}
	if not bool(config.get("enabled", False)):
		return None

	runtime_hours = max(float(total_runtime_seconds or 0.0), 0.0) / 3600.0
	if runtime_hours <= 0:
		return {
			"enabled": True,
			"note": "UBELIX estimate skipped (runtime was zero).",
		}

	pue_raw = config.get("pue", 1.0)
	try:
		pue = max(1.0, float(pue_raw))
	except Exception:
		pue = 1.0

	usage_raw = config.get("core_usage_factor", 1.0)
	try:
		core_usage_factor = min(max(float(usage_raw), 0.0), 1.0)
	except Exception:
		core_usage_factor = 1.0

	mult_raw = config.get("multiplicative_factor", 1.0)
	try:
		multiplicative_factor = max(float(mult_raw), 1.0)
	except Exception:
		multiplicative_factor = 1.0

	memory_gb_raw = config.get("memory_gb", 0.0)
	try:
		memory_gb = max(float(memory_gb_raw), 0.0)
	except Exception:
		memory_gb = 0.0

	memory_w_per_gb_raw = config.get("memory_power_watts_per_gb", 0.0)
	try:
		memory_power_watts_per_gb = max(float(memory_w_per_gb_raw), 0.0)
	except Exception:
		memory_power_watts_per_gb = 0.0

	memory_power_watts = memory_gb * memory_power_watts_per_gb

	grid_raw = config.get("grid_carbon_intensity_g_per_kwh", None)
	grid_intensity = None
	try:
		if grid_raw is not None:
			grid_intensity = float(grid_raw)
	except Exception:
		grid_intensity = None

	tdp_table = config.get("resource_tdp_watts", {}) if isinstance(config.get("resource_tdp_watts"), dict) else {}
	usage_table = config.get("resource_usage", {}) if isinstance(config.get("resource_usage"), dict) else {}

	it_power_watts = 0.0
	breakdown: dict[str, dict[str, float]] = {}

	for resource_name, count_raw in usage_table.items():
		try:
			count = max(float(count_raw), 0.0)
		except Exception:
			continue
		try:
			tdp_watts = max(float(tdp_table.get(resource_name, 0.0) or 0.0), 0.0)
		except Exception:
			tdp_watts = 0.0
		if count <= 0 or tdp_watts <= 0:
			continue

		resource_power_watts = count * tdp_watts
		resource_energy_kwh = (resource_power_watts * runtime_hours) / 1000.0
		breakdown[resource_name] = {
			"count": count,
			"tdp_watts": tdp_watts,
			"power_watts": resource_power_watts,
			"it_energy_kwh": resource_energy_kwh,
		}
		it_power_watts += resource_power_watts

	core_power_watts_usage_adjusted = it_power_watts * core_usage_factor
	it_power_watts_total = core_power_watts_usage_adjusted + memory_power_watts
	it_energy_kwh = (it_power_watts_total * runtime_hours) / 1000.0
	facility_energy_kwh = it_energy_kwh * pue * multiplicative_factor
	operational_co2e_kg = None
	if grid_intensity is not None:
		operational_co2e_kg = (facility_energy_kwh * grid_intensity) / 1000.0

	return {
		"enabled": True,
		"runtime_hours": runtime_hours,
		"resource_breakdown": breakdown,
		"core_power_watts_raw": it_power_watts,
		"core_usage_factor": core_usage_factor,
		"core_power_watts_usage_adjusted": core_power_watts_usage_adjusted,
		"memory_gb": memory_gb,
		"memory_power_watts_per_gb": memory_power_watts_per_gb,
		"memory_power_watts": memory_power_watts,
		"multiplicative_factor": multiplicative_factor,
		"it_power_watts": it_power_watts_total,
		"it_energy_kwh": it_energy_kwh,
		"pue": pue,
		"facility_energy_kwh": facility_energy_kwh,
		"grid_carbon_intensity_g_per_kwh": grid_intensity,
		"operational_co2e_kg": operational_co2e_kg,
		"note": "Rough operational estimate only; excludes embodied emissions (materials/manufacturing/transport).",
	}


def _build_ubelix_assumption_log() -> dict[str, Any] | None:
	"""Build a compact assumption log for reproducibility/audit reporting."""

	config = UBELIX_ESTIMATION_CONFIG if isinstance(UBELIX_ESTIMATION_CONFIG, dict) else {}
	if not bool(config.get("enabled", False)):
		return None

	assumptions = config.get("assumptions", {}) if isinstance(config.get("assumptions"), dict) else {}
	return {
		"method_reference": "https://calculator.green-algorithms.org/",
		"scope": "Operational estimate only (no embodied emissions).",
		"pue": config.get("pue"),
		"grid_carbon_intensity_g_per_kwh": config.get("grid_carbon_intensity_g_per_kwh"),
		"core_usage_factor": config.get("core_usage_factor", 1.0),
		"memory_gb": config.get("memory_gb", 0.0),
		"memory_power_watts_per_gb": config.get("memory_power_watts_per_gb", 0.0),
		"multiplicative_factor": config.get("multiplicative_factor", 1.0),
		"resource_usage": config.get("resource_usage"),
		"resource_tdp_watts": config.get("resource_tdp_watts"),
		"sources": {
			"pue_source": assumptions.get("pue_source", ""),
			"pue_source_date": assumptions.get("pue_source_date", ""),
			"grid_intensity_source": assumptions.get("grid_intensity_source", ""),
			"grid_intensity_source_date": assumptions.get("grid_intensity_source_date", ""),
			"resource_usage_source": assumptions.get("resource_usage_source", ""),
			"resource_usage_source_date": assumptions.get("resource_usage_source_date", ""),
			"core_usage_factor_source": assumptions.get("core_usage_factor_source", ""),
			"core_usage_factor_source_date": assumptions.get("core_usage_factor_source_date", ""),
			"memory_source": assumptions.get("memory_source", ""),
			"memory_source_date": assumptions.get("memory_source_date", ""),
			"multiplicative_factor_source": assumptions.get("multiplicative_factor_source", ""),
			"multiplicative_factor_source_date": assumptions.get("multiplicative_factor_source_date", ""),
			"notes": assumptions.get("notes", ""),
		},
	}


def _ubelix_assumption_missing_fields(assumption_log: dict[str, Any] | None) -> list[str]:
	"""Return required assumption source fields that are still blank."""

	if not assumption_log or not isinstance(assumption_log, dict):
		return []
	sources = assumption_log.get("sources", {}) if isinstance(assumption_log.get("sources"), dict) else {}
	required_fields = [
		"pue_source",
		"pue_source_date",
		"grid_intensity_source",
		"grid_intensity_source_date",
		"resource_usage_source",
		"resource_usage_source_date",
		"core_usage_factor_source",
		"core_usage_factor_source_date",
		"memory_source",
		"memory_source_date",
		"multiplicative_factor_source",
		"multiplicative_factor_source_date",
	]
	missing: list[str] = []
	for field in required_fields:
		value = sources.get(field, "")
		if not str(value).strip():
			missing.append(field)
	return missing


def _print_ubelix_summary_line(total_runtime_seconds: float, stage: str, run_label: str) -> None:
	"""Print one operator-friendly summary line after a run."""

	assumption_log = _build_ubelix_assumption_log()
	if not assumption_log:
		return

	estimate = _estimate_ubelix_operational(total_runtime_seconds)
	if not estimate or not isinstance(estimate, dict):
		return

	energy_kwh = estimate.get("facility_energy_kwh")
	co2e_kg = estimate.get("operational_co2e_kg")
	missing = _ubelix_assumption_missing_fields(assumption_log)
	assumption_status = "complete" if not missing else f"missing({','.join(missing)})"

	energy_text = f"{float(energy_kwh):.4f} kWh" if isinstance(energy_kwh, (int, float)) else "n/a"
	co2_text = f"{float(co2e_kg):.4f} kgCO2e" if isinstance(co2e_kg, (int, float)) else "n/a"

	print(
		f"[ubelix] stage={stage} run={run_label} estimate={energy_text}, {co2_text} assumptions={assumption_status}",
		flush=True,
	)


def _count_qc_papers(qc_sample_path: Path | None) -> int:
	"""Count QC sample rows (header excluded)."""

	if not qc_sample_path:
		return 0
	path = Path(qc_sample_path)
	if not path.exists():
		return 0
	try:
		with path.open("r", encoding="utf-8") as handle:
			reader = csv.DictReader(handle)
			return sum(1 for _ in reader)
	except Exception as exc:  # pylint: disable=broad-except
		logging.warning("Failed to read QC sample at %s: %s", path, exc)
		return 0

try:
	from codecarbon import EmissionsTracker, OfflineEmissionsTracker  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dependency
	raise RuntimeError("CodeCarbon is required but not installed; install codecarbon before running the pipeline.")


@dataclass
class ResourceUsageConfig:
	"""Configuration for resource usage tracking.

	Args:
		resource_log_path: Path to JSONL resource log.
		enable_tracking: If True, write resource logs and totals.
		enable_codecarbon: If True, track emissions via CodeCarbon (if installed).
		stage: Current pipeline stage (title_abstract | full_text | data_extraction).
		qc_sample_path: Optional QC sample CSV path to derive actual QC counts.
		qc_paper_count: Optional precomputed QC size to avoid re-reading the QC CSV.
		run_label: Run label suffix (qc_sample or remaining_sample) for file naming.
		enable_time_savings: If True, compute human-time savings (only when validation ran).
	"""

	resource_log_path: Path
	enable_tracking: bool = True
	enable_codecarbon: bool = True
	stage: str = "title_abstract"
	qc_sample_path: Path | None = None
	qc_paper_count: int | None = None
	run_label: str = "run"
	enable_time_savings: bool = False


class CarbonTrackerManager:
	"""Initialize and manage CodeCarbon trackers with offline/online support."""

	def __init__(self, enabled: bool = True) -> None:
		self._enabled = enabled
		self._tracker = None
		self._started = False
		self._init_tracker()

	def _init_tracker(self) -> None:
		if not self._enabled:
			raise RuntimeError("CodeCarbon tracking was requested but disabled at construction time.")

		try:
			output_dir = Path(CARBON_CONFIG["output_dir"])
			output_dir.mkdir(parents=True, exist_ok=True)

			tracker_kwargs = {
				"project_name": CARBON_CONFIG["project_name"],
				"output_dir": str(output_dir),
				"measure_power_secs": CARBON_CONFIG["measure_power_secs"],
				"tracking_mode": CARBON_CONFIG["tracking_mode"],
				"on_csv_write": CARBON_CONFIG["on_csv_write"],
				"save_to_file": True,
			}

			pue = CARBON_CONFIG.get("pue")
			if pue is not None:
				tracker_kwargs["pue"] = pue
			wue = CARBON_CONFIG.get("wue")
			if wue is not None:
				tracker_kwargs["wue"] = wue

			if CARBON_CONFIG["is_offline"]:
				country_code = CARBON_CONFIG.get("country_iso_code")
				if not country_code:
					raise ValueError("CARBON_CONFIG['country_iso_code'] is required when is_offline=True.")
				if OfflineEmissionsTracker is None:
					raise RuntimeError("OfflineEmissionsTracker unavailable; install codecarbon with offline support.")
				self._tracker = OfflineEmissionsTracker(country_iso_code=country_code, **tracker_kwargs)
			else:
				self._tracker = EmissionsTracker(**tracker_kwargs)
		except Exception as exc:  # pylint: disable=broad-except
			logging.error("CodeCarbon tracker initialization failed: %s", exc)
			raise

	def start(self) -> None:
		"""Start the tracker (no-op if unavailable)."""
		if not self._enabled:
			raise RuntimeError("CodeCarbon tracking disabled; cannot start tracker.")
		if self._tracker is None:
			raise RuntimeError("CodeCarbon tracker missing; initialization must succeed before start.")
		if self._started:
			return
		try:
			self._tracker.start()
			self._started = True
		except Exception as exc:  # pylint: disable=broad-except
			logging.error("CodeCarbon tracker start failed: %s", exc)
			raise

	def stop(self) -> float | None:
		"""Stop the tracker and return emissions (kg CO2eq), if available."""
		if not self._enabled or self._tracker is None or not self._started:
			return None
		try:
			return self._tracker.stop()
		except Exception as exc:  # pylint: disable=broad-except
			logging.warning("CodeCarbon tracker stop failed: %s", exc)
			return None
		finally:
			self._started = False

	def rename_emissions_csv(self, timestamp_label: str | None = None, run_label: str | None = None) -> Path | None:
		"""Rename CodeCarbon's emissions.csv to stage/sample naming: <stage>_<sample>_codecarbon_emissions_<timestamp>."""

		output_dir = Path(CARBON_CONFIG["output_dir"])
		source = output_dir / "emissions.csv"
		if not source.exists():
			return None

		stage = output_dir.name
		stamp = timestamp_label or datetime.now().strftime("%Y%m%d_%H-%M")
		sample_tag = None
		if run_label:
			sample_tag = run_label.replace("_sample", "") if run_label.endswith("_sample") else run_label
		sample_part = f"{sample_tag}_sample" if sample_tag else "run"
		target = output_dir / f"{stage}_{sample_part}_codecarbon_emissions_{stamp}.csv"

		if target.exists():
			for idx in range(1, 1000):
				candidate = output_dir / f"{stage}_{sample_part}_codecarbon_emissions_{stamp}_{idx}.csv"
				if not candidate.exists():
					target = candidate
					break

		try:
			source.replace(target)
		except Exception as exc:  # pylint: disable=broad-except
			logging.warning("Failed to rename CodeCarbon emissions.csv: %s", exc)
			return None
		return target

	def energy_kwh(self) -> float | None:
		"""Return final energy consumed in kWh, if available."""

		if self._tracker is None:
			return None
		data = getattr(self._tracker, "final_emissions_data", None)
		return getattr(data, "energy_consumed", None) if data is not None else None

	def __enter__(self) -> "CarbonTrackerManager":
		self.start()
		return self

	def __exit__(self, exc_type, exc, tb) -> bool:
		self.stop()
		return False

	@staticmethod
	def measure_energy(func):
		"""Decorator for function-level emissions tracking."""

		def wrapper(*args, **kwargs):
			with CarbonTrackerManager():
				return func(*args, **kwargs)

		return wrapper


class ResourceUsageTracker:
	"""Track per-paper and per-run resource usage, with optional CodeCarbon."""

	def __init__(self, config: ResourceUsageConfig) -> None:
		self.config = config
		self.stage = config.stage
		self.qc_sample_path = config.qc_sample_path
		self._qc_paper_count: int | None = getattr(config, "qc_paper_count", None)
		self._resource_totals = {
			"tokens_total": 0,
			"prompt_tokens": 0,
			"response_tokens": 0,
			"embedding_tokens": 0,
			"pdf_text_tokens": 0,
			"pdf_visual_tokens": 0,
			"paper_seconds": 0.0,
		}
		self._paper_records: list[dict] = []
		self._carbon_tracker: Optional[CarbonTrackerManager] = None

	def start_run(self) -> None:
		"""Start CodeCarbon tracking (if enabled and available)."""

		if not self.config.enable_tracking or not self.config.enable_codecarbon:
			return
		if self._carbon_tracker is None:
			self._carbon_tracker = CarbonTrackerManager(enabled=True)
		self._carbon_tracker.start()

	def set_qc_count(self, qc_count: int) -> None:
		"""Allow callers to set QC paper count without re-reading the QC CSV."""

		if qc_count is None:
			return
		try:
			self._qc_paper_count = int(qc_count)
		except Exception:
			self._qc_paper_count = None

	def stop_run(self, total_runtime_seconds: float, paper_count: int) -> None:
		"""Stop CodeCarbon tracking and append per-run totals."""

		if not self.config.enable_tracking:
			return
		emissions_kg = None
		energy_kwh = None
		if self._carbon_tracker is not None:
			emissions_kg = self._carbon_tracker.stop()
			energy_kwh = self._carbon_tracker.energy_kwh()
			self._carbon_tracker.rename_emissions_csv(run_label=self.config.run_label)
		self._write_totals(total_runtime_seconds, paper_count, emissions_kg, energy_kwh)
		_print_ubelix_summary_line(total_runtime_seconds, self.stage, self.config.run_label)

	def log_paper(
		self,
		paper_id: str,
		prompt_tokens: int,
		response_tokens: int,
		pdf_text_tokens: int = 0,
		pdf_visual_tokens: int = 0,
		embedding_tokens: int = 0,
		prompt_tokens_source: str = "estimate",
		response_tokens_source: str = "estimate",
		embedding_tokens_source: str = "estimate",
		paper_seconds: float | None = None,
	) -> None:
		"""Append per-paper resource usage to the JSONL log (prefers API token counts when available)."""

		if not self.config.enable_tracking:
			return

		total_tokens = max(prompt_tokens + response_tokens + embedding_tokens + pdf_text_tokens + pdf_visual_tokens, 0)

		record = {
			"paper_id": paper_id,
			"tokens_total": total_tokens,
			"prompt_tokens": prompt_tokens,
			"response_tokens": response_tokens,
			"embedding_tokens": embedding_tokens,
			"prompt_tokens_source": prompt_tokens_source,
			"response_tokens_source": response_tokens_source,
			"embedding_tokens_source": embedding_tokens_source,
			"pdf_text_tokens": pdf_text_tokens,
			"pdf_visual_tokens": pdf_visual_tokens,
			"timestamp": datetime.utcnow().isoformat(),
			"paper_seconds": paper_seconds,
		}
		self._paper_records.append(record)

		self._resource_totals["tokens_total"] += total_tokens
		self._resource_totals["prompt_tokens"] += prompt_tokens
		self._resource_totals["response_tokens"] += response_tokens
		self._resource_totals["embedding_tokens"] += embedding_tokens
		self._resource_totals["pdf_text_tokens"] += pdf_text_tokens
		self._resource_totals["pdf_visual_tokens"] += pdf_visual_tokens
		if paper_seconds is not None:
			self._resource_totals["paper_seconds"] += paper_seconds

	def _write_totals(
		self,
		total_runtime_seconds: float,
		paper_count: int,
		emissions_kg: float | None,
		energy_kwh: float | None,
	) -> None:
		"""Append buffered per-paper entries plus per-run totals in one write."""

		timestamp = datetime.utcnow().isoformat()
		total_runtime_avg_seconds_per_paper = (total_runtime_seconds / paper_count) if paper_count else 0.0
		self_enabled = getattr(self.config, "enable_time_savings", False)
		human_rate_min_per_paper = None
		human_minutes_estimate = None
		time_saved_minutes = None
		time_saved_percent = None
		time_saved_note = None

		if self_enabled:
			stage_cfg = HUMAN_TIME_CONFIG.get(self.stage, {}) if isinstance(HUMAN_TIME_CONFIG, dict) else {}
			qc_papers = self._resolve_qc_papers(stage_cfg)
			reviewers = stage_cfg.get("reviewers") or []
			per_reviewer_rates = []
			if qc_papers > 0:
				for reviewer in reviewers:
					total_minutes = reviewer.get("total_minutes") if isinstance(reviewer, dict) else None
					if total_minutes is None:
						continue
					try:
						minutes_val = float(total_minutes)
					except Exception:
						continue
					if minutes_val <= 0:
						continue
					per_reviewer_rates.append(minutes_val / qc_papers)
			if per_reviewer_rates:
				human_rate_min_per_paper = sum(per_reviewer_rates) / len(per_reviewer_rates)
				human_minutes_estimate = human_rate_min_per_paper * paper_count
				pipeline_minutes = total_runtime_seconds / 60.0
				time_saved_minutes = human_minutes_estimate - pipeline_minutes
				if human_minutes_estimate > 0:
					time_saved_percent = 1.0 - (pipeline_minutes / human_minutes_estimate)
			elif qc_papers > 0:
				time_saved_note = "time-savings skipped (no reviewer minutes provided)"
		else:
			time_saved_note = "time-savings skipped (validation not run)"
		total_energy_kwh = energy_kwh
		total_carbon_g = (emissions_kg * 1000.0) if emissions_kg is not None else None
		cc_intensity = (total_carbon_g / total_energy_kwh) if total_carbon_g and total_energy_kwh else None
		cc_energy_per_1k_tokens = None
		cc_carbon_g_per_1k_tokens = None
		if emissions_kg is not None and energy_kwh is not None and self._resource_totals["tokens_total"]:
			tokens_total = self._resource_totals["tokens_total"]
			cc_energy_per_1k_tokens = (energy_kwh / tokens_total) * 1000.0
			cc_carbon_g_per_1k_tokens = ((emissions_kg * 1000.0) / tokens_total) * 1000.0

		ubelix_estimate = _estimate_ubelix_operational(total_runtime_seconds)
		ubelix_assumptions_log = _build_ubelix_assumption_log()

		self.config.resource_log_path.parent.mkdir(parents=True, exist_ok=True)
		entries: list[str] = []
		for record in self._paper_records:
			entries.append(json.dumps(record) + "\n")
		entries.append(
			json.dumps(
				{
					"paper_id": "TOTAL",
					"tokens_total": self._resource_totals["tokens_total"],
					"prompt_tokens": self._resource_totals["prompt_tokens"],
					"response_tokens": self._resource_totals["response_tokens"],
					"embedding_tokens": self._resource_totals["embedding_tokens"],
					"pdf_text_tokens": self._resource_totals["pdf_text_tokens"],
					"pdf_visual_tokens": self._resource_totals["pdf_visual_tokens"],
					"codecarbon_emissions_kg": emissions_kg,
					"codecarbon_energy_kwh": energy_kwh,
					"codecarbon_energy_kwh_per_1k_tokens": cc_energy_per_1k_tokens,
					"codecarbon_carbon_g_per_1k_tokens": cc_carbon_g_per_1k_tokens,
					"codecarbon_carbon_intensity_g_per_kwh": cc_intensity,
					"ubelix_operational_estimate": ubelix_estimate,
					"ubelix_assumptions_log": ubelix_assumptions_log,
					"total_runtime_seconds": total_runtime_seconds,
					"total_runtime_avg_seconds_per_paper": total_runtime_avg_seconds_per_paper,
					"paper_count": paper_count,
					"paper_seconds_total": self._resource_totals.get("paper_seconds", 0.0),
					"llm_decision_avg_seconds_per_paper": (self._resource_totals.get("paper_seconds", 0.0) / paper_count)
					if paper_count
					else 0.0,
					"human_rate_min_per_paper": human_rate_min_per_paper,
					"human_minutes_estimate": human_minutes_estimate,
					"time_saved_minutes": time_saved_minutes,
					"time_saved_percent": time_saved_percent,
					"time_saved_note": time_saved_note,
					"timestamp": timestamp,
				}
			)
			+ "\n"
		)
		with open(self.config.resource_log_path, "w", encoding="utf-8") as logf:
			logf.writelines(entries)

	def _resolve_qc_papers(self, stage_cfg: dict) -> int:
		"""Determine QC paper count from the QC sample file; falls back to zero if unavailable."""

		if self._qc_paper_count is not None:
			return self._qc_paper_count
		return _count_qc_papers(self.qc_sample_path)


class ResourceUsageEngine:
	"""human readable hint: dominant class for this script; it exposes one stable API for run/resource tracking."""

	def __init__(
		self,
		resource_log_path: Path,
		enable_tracking: bool = True,
		enable_codecarbon: bool = True,
		stage: str = "title_abstract",
		qc_sample_path: Path | None = None,
		qc_paper_count: int | None = None,
		run_label: str = "run",
		enable_time_savings: bool = False,
	) -> None:
		"""human readable hint: __init__ captures all run-level tracking parameters in one visible constructor."""

		self.config = ResourceUsageConfig(
			resource_log_path=resource_log_path,
			enable_tracking=enable_tracking,
			enable_codecarbon=enable_codecarbon,
			stage=stage,
			qc_sample_path=qc_sample_path,
			qc_paper_count=qc_paper_count,
			run_label=run_label,
			enable_time_savings=enable_time_savings,
		)
		self._tracker = ResourceUsageTracker(self.config)

	def start_run(self) -> None:
		"""human readable hint: start CodeCarbon/resource tracking for the current run."""

		self._tracker.start_run()

	def set_qc_count(self, qc_count: int) -> None:
		"""human readable hint: set QC paper count once so the tracker does not re-read QC CSV files."""

		self._tracker.set_qc_count(qc_count)

	def log_paper(
		self,
		paper_id: str,
		prompt_tokens: int,
		response_tokens: int,
		pdf_text_tokens: int = 0,
		pdf_visual_tokens: int = 0,
		embedding_tokens: int = 0,
		prompt_tokens_source: str = "estimate",
		response_tokens_source: str = "estimate",
		embedding_tokens_source: str = "estimate",
		paper_seconds: float | None = None,
	) -> None:
		"""human readable hint: log per-paper token/runtime metrics in the shared run tracker."""

		self._tracker.log_paper(
			paper_id=paper_id,
			prompt_tokens=prompt_tokens,
			response_tokens=response_tokens,
			pdf_text_tokens=pdf_text_tokens,
			pdf_visual_tokens=pdf_visual_tokens,
			embedding_tokens=embedding_tokens,
			prompt_tokens_source=prompt_tokens_source,
			response_tokens_source=response_tokens_source,
			embedding_tokens_source=embedding_tokens_source,
			paper_seconds=paper_seconds,
		)

	def stop_run(self, total_runtime_seconds: float, paper_count: int) -> None:
		"""human readable hint: stop tracking and write final TOTAL summary lines."""

		self._tracker.stop_run(total_runtime_seconds=total_runtime_seconds, paper_count=paper_count)


def backfill_time_savings(resource_log_path: Path, stage: str, qc_sample_path: Path | None) -> bool:
	"""Recompute human-time fields in an existing resource_usage log after minutes are confirmed.

	Returns True if the log was updated.
	"""

	if not resource_log_path or not Path(resource_log_path).exists():
		return False

	try:
		lines = Path(resource_log_path).read_text(encoding="utf-8").splitlines()
	except Exception:
		return False

	parsed: list[dict[str, Any] | str] = []
	for line in lines:
		try:
			parsed.append(json.loads(line))
		except Exception:
			parsed.append(line)

	last_total_idx = None
	for idx, obj in enumerate(parsed):
		if isinstance(obj, dict) and obj.get("paper_id") == "TOTAL":
			last_total_idx = idx
	if last_total_idx is None:
		return False

	total_entry = parsed[last_total_idx]
	if not isinstance(total_entry, dict):
		return False

	paper_count = total_entry.get("paper_count")
	total_runtime_seconds = total_entry.get("total_runtime_seconds")
	if not isinstance(paper_count, (int, float)) or not isinstance(total_runtime_seconds, (int, float)):
		return False

	stage_cfg = HUMAN_TIME_CONFIG.get(stage, {}) if isinstance(HUMAN_TIME_CONFIG, dict) else {}
	qc_papers = _count_qc_papers(qc_sample_path)
	reviewers = stage_cfg.get("reviewers") or []
	per_reviewer_rates: list[float] = []
	if qc_papers > 0:
		for reviewer in reviewers:
			total_minutes = reviewer.get("total_minutes") if isinstance(reviewer, dict) else None
			if total_minutes is None:
				continue
			try:
				minutes_val = float(total_minutes)
			except Exception:
				continue
			if minutes_val <= 0:
				continue
			per_reviewer_rates.append(minutes_val / qc_papers)

	human_rate_min_per_paper = None
	human_minutes_estimate = None
	time_saved_minutes = None
	time_saved_percent = None
	time_saved_note = None

	if per_reviewer_rates:
		human_rate_min_per_paper = sum(per_reviewer_rates) / len(per_reviewer_rates)
		human_minutes_estimate = human_rate_min_per_paper * paper_count
		pipeline_minutes = total_runtime_seconds / 60.0
		time_saved_minutes = human_minutes_estimate - pipeline_minutes
		if human_minutes_estimate > 0:
			time_saved_percent = 1.0 - (pipeline_minutes / human_minutes_estimate)
	elif qc_papers > 0:
		time_saved_note = "time-savings skipped (no reviewer minutes provided)"
	else:
		time_saved_note = "time-savings skipped (no QC sample detected)"

	total_entry.update(
		{
			"human_rate_min_per_paper": human_rate_min_per_paper,
			"human_minutes_estimate": human_minutes_estimate,
			"time_saved_minutes": time_saved_minutes,
			"time_saved_percent": time_saved_percent,
			"time_saved_note": time_saved_note,
		}
	)

	out_lines: list[str] = []
	for obj in parsed:
		if isinstance(obj, dict):
			out_lines.append(json.dumps(obj) + "\n")
		elif isinstance(obj, str):
			out_lines.append(obj + "\n")
		else:
			continue

	try:
		with Path(resource_log_path).open("w", encoding="utf-8") as handle:
			handle.writelines(out_lines)
	except Exception:
		return False

	return True