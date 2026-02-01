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
from typing import Optional

from config.user_orchestrator import CARBON_CONFIG, HUMAN_TIME_CONFIG

try:
	from codecarbon import EmissionsTracker, OfflineEmissionsTracker  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dependency
	EmissionsTracker = None
	OfflineEmissionsTracker = None


@dataclass
class ResourceUsageConfig:
	"""Configuration for resource usage tracking.

	Args:
		resource_log_path: Path to JSONL resource log.
		enable_tracking: If True, write resource logs and totals.
		enable_codecarbon: If True, track emissions via CodeCarbon (if installed).
		stage: Current pipeline stage (title_abstract | full_text | data_extraction).
		qc_sample_path: Optional QC sample CSV path to derive actual QC counts.
		run_label: Run label suffix (qc_sample or remaining_sample) for file naming.
	"""

	resource_log_path: Path
	enable_tracking: bool = True
	enable_codecarbon: bool = True
	stage: str = "title_abstract"
	qc_sample_path: Path | None = None
	run_label: str = "run"


class CarbonTrackerManager:
	"""Initialize and manage CodeCarbon trackers with offline/online support."""

	def __init__(self, enabled: bool = True) -> None:
		self._enabled = enabled
		self._tracker = None
		self._started = False
		self._init_tracker()

	def _init_tracker(self) -> None:
		if not self._enabled:
			return
		if EmissionsTracker is None:
			logging.warning("CodeCarbon is not installed; emissions tracking disabled.")
			return

		try:
			output_dir = Path(CARBON_CONFIG["output_dir"])
			output_dir.mkdir(parents=True, exist_ok=True)

			tracker_kwargs = {
				"project_name": CARBON_CONFIG["project_name"],
				"output_dir": str(output_dir),
				"measure_power_secs": CARBON_CONFIG["measure_power_secs"],
				"tracking_mode": CARBON_CONFIG["tracking_mode"],
				"on_csv_write": CARBON_CONFIG["on_csv_write"],
				"save_to_file": CARBON_CONFIG["save_to_file"],
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
			logging.warning("CodeCarbon tracker initialization failed: %s", exc)
			self._tracker = None

	def start(self) -> None:
		"""Start the tracker (no-op if unavailable)."""
		if not self._enabled or self._tracker is None or self._started:
			return
		try:
			self._tracker.start()
			self._started = True
		except Exception as exc:  # pylint: disable=broad-except
			logging.warning("CodeCarbon tracker start failed: %s", exc)
			self._tracker = None

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
		"""Rename CodeCarbon's emissions.csv to a stage-scoped, timestamped filename."""

		output_dir = Path(CARBON_CONFIG["output_dir"])
		source = output_dir / "emissions.csv"
		if not source.exists():
			return None

		stage = output_dir.name
		tracking_mode = CARBON_CONFIG.get("tracking_mode", "machine")
		stamp = timestamp_label or datetime.now().strftime("%Y%m%d_%H-%M")
		run_part = run_label or "run"
		target = output_dir / f"{stage}_codecarbon_emissions_{run_part}_{tracking_mode}_{stamp}.csv"

		if target.exists():
			for idx in range(1, 1000):
				candidate = output_dir / f"{stage}_codecarbon_emissions_{tracking_mode}_{stamp}_{idx}.csv"
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
		self._resource_totals = {
			"tokens_total": 0,
			"prompt_tokens": 0,
			"response_tokens": 0,
			"embedding_tokens": 0,
			"pdf_text_tokens": 0,
			"pdf_visual_tokens": 0,
			"paper_seconds": 0.0,
		}
		self._carbon_tracker: Optional[CarbonTrackerManager] = None

	def start_run(self) -> None:
		"""Start CodeCarbon tracking (if enabled and available)."""

		if not self.config.enable_tracking or not self.config.enable_codecarbon:
			return
		if self._carbon_tracker is None:
			self._carbon_tracker = CarbonTrackerManager(enabled=True)
		self._carbon_tracker.start()

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

		self.config.resource_log_path.parent.mkdir(parents=True, exist_ok=True)
		with open(self.config.resource_log_path, "a", encoding="utf-8") as logf:
			logf.write(
				json.dumps(
					{
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
				)
				+ "\n"
			)

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
		"""Append per-run totals (including CodeCarbon, if available)."""

		timestamp = datetime.utcnow().isoformat()
		total_runtime_avg_seconds_per_paper = (total_runtime_seconds / paper_count) if paper_count else 0.0
		human_rate_min_per_paper = None
		human_minutes_estimate = None
		time_saved_minutes = None
		time_saved_percent = None
		time_saved_note = None

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
		else:
			if qc_papers > 0:
				time_saved_note = "time-savings skipped (no reviewer minutes provided)"
		total_energy_kwh = energy_kwh
		total_carbon_g = (emissions_kg * 1000.0) if emissions_kg is not None else None
		cc_intensity = (total_carbon_g / total_energy_kwh) if total_carbon_g and total_energy_kwh else None
		cc_energy_per_1k_tokens = None
		cc_carbon_g_per_1k_tokens = None
		if emissions_kg is not None and energy_kwh is not None and self._resource_totals["tokens_total"]:
			tokens_total = self._resource_totals["tokens_total"]
			cc_energy_per_1k_tokens = (energy_kwh / tokens_total) * 1000.0
			cc_carbon_g_per_1k_tokens = ((emissions_kg * 1000.0) / tokens_total) * 1000.0

		self.config.resource_log_path.parent.mkdir(parents=True, exist_ok=True)
		with open(self.config.resource_log_path, "a", encoding="utf-8") as logf:
			logf.write(
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

	def _resolve_qc_papers(self, stage_cfg: dict) -> int:
		"""Determine QC paper count from the QC sample file; falls back to zero if unavailable."""

		qc_path = self.qc_sample_path
		if qc_path:
			path = Path(qc_path)
			if path.exists():
				try:
					with open(path, "r", encoding="utf-8") as handle:
						reader = csv.DictReader(handle)
						return sum(1 for _ in reader)
				except Exception as exc:  # pylint: disable=broad-except
					logging.warning("Failed to read QC sample at %s: %s", path, exc)

		# No QC file or unreadable: treat as zero so time-savings is skipped with a clear note.
		return 0