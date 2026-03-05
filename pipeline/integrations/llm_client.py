"""LLM client helper (moved from llm/main.py)."""

from __future__ import annotations

import asyncio
import random
import sys
import time
from typing import Any

import openai

from config.user_orchestrator import LLM_SETTINGS, require_setting


def _format_prompt(template: str, data: str) -> str:
	"""Insert the evidence text into the prompt template."""

	return template.replace("{data}", data)


class OpenAIResponder:
	"""Generate responses using the OpenAI API within a RAG workflow."""

	def __init__(self, data: str, model: str, prompt_template: str, client: Any) -> None:
		self.data = data
		self.model = model
		self.prompt_template = prompt_template
		self.client = client
		self.prompt = _format_prompt(prompt_template, data)

	def _request_kwargs(self) -> dict[str, Any]:
		"""human readable hint: build one consistent chat request payload for sync and async calls."""

		request_kwargs: dict[str, Any] = {
			"model": self.model,
			"messages": [
				{"role": "system", "content": "You are a RAG system."},
				{"role": "user", "content": self.prompt},
			],
			"max_tokens": require_setting(LLM_SETTINGS, "max_tokens", "LLM_SETTINGS", int),
			"temperature": require_setting(LLM_SETTINGS, "temperature", "LLM_SETTINGS", float),
			"top_p": float(LLM_SETTINGS.get("top_p", 1.0) or 1.0),
			"stream": False,
		}

		seed_val = LLM_SETTINGS.get("seed")
		if isinstance(seed_val, int):
			request_kwargs["seed"] = seed_val

		return request_kwargs

	@staticmethod
	def _usage_to_dict(usage: Any) -> dict[str, Any] | None:
		"""human readable hint: normalize provider usage objects into plain dictionaries."""

		if usage is None:
			return None
		if hasattr(usage, "model_dump"):
			return usage.model_dump()
		try:
			return dict(usage)
		except Exception:
			return None

	@staticmethod
	def _response_to_tuple(response: Any) -> tuple[str, dict[str, Any] | None]:
		"""human readable hint: parse one response object and return content plus usage metadata."""

		if not response or not getattr(response, "choices", None):
			raise RuntimeError("Empty response from LLM (no choices)")

		message = response.choices[0].message if response.choices[0] else None
		content = message.content if message else None
		if content is None:
			raise RuntimeError("Empty response from LLM (no message content)")

		text = content.strip()
		if not text:
			raise RuntimeError("Empty response from LLM (blank content)")

		return text, OpenAIResponder._usage_to_dict(getattr(response, "usage", None))

	@staticmethod
	def _is_retryable_error(exc: Exception) -> bool:
		"""human readable hint: retry only on transient transport/rate-limit provider failures."""

		if isinstance(
			exc,
			(
				openai.RateLimitError,
				openai.APITimeoutError,
				openai.APIConnectionError,
				openai.InternalServerError,
			),
		):
			return True
		status_code = getattr(exc, "status_code", None)
		return bool(status_code in {429, 500, 502, 503, 504})

	def generate_response(self, retries: int = 1, backoff_seconds: float = 0.5) -> tuple[str, dict | None]:
		"""Get one response from the model and return text plus usage metadata."""

		last_error: Exception | None = None
		for attempt in range(retries + 1):
			try:
				response = self.client.chat.completions.create(**self._request_kwargs())
				return self._response_to_tuple(response)
			except Exception as exc:  # pylint: disable=broad-except
				last_error = exc
				print(
					f"[error] openai chat attempt {attempt + 1}/{retries + 1} failed for model='{self.model}': {exc}",
					file=sys.stderr,
				)
				if attempt < retries:
					time.sleep(backoff_seconds)
					continue
			raise RuntimeError(
				f"LLM response generation failed for model='{self.model}' after {retries + 1} attempt(s): {last_error}"
			)

		raise RuntimeError("LLM response generation failed without a response.")

	async def generate_response_async(
		self,
		max_retries: int = 3,
		backoff_base_seconds: float = 0.5,
		backoff_max_seconds: float = 8.0,
		jitter_seconds: float = 0.1,
	) -> tuple[str, dict[str, Any] | None]:
		"""Get one async response with retry/backoff for transient provider errors."""

		last_error: Exception | None = None
		for attempt in range(max_retries + 1):
			try:
				response = await self.client.chat.completions.create(**self._request_kwargs())
				return self._response_to_tuple(response)
			except Exception as exc:  # pylint: disable=broad-except
				last_error = exc
				retryable = self._is_retryable_error(exc)
				if attempt < max_retries and retryable:
					delay = min(backoff_max_seconds, backoff_base_seconds * (2 ** attempt))
					delay += random.uniform(0.0, max(0.0, jitter_seconds))
					print(
						f"[warn] async chat retry {attempt + 1}/{max_retries + 1} for model='{self.model}' after transient error: {exc}",
						file=sys.stderr,
					)
					await asyncio.sleep(delay)
					continue
				raise RuntimeError(
					f"Async LLM response generation failed for model='{self.model}' after {attempt + 1} attempt(s): {last_error}"
				)

		raise RuntimeError("Async LLM response generation failed without a response.")
