"""LLM client helper (moved from llm/main.py)."""

from __future__ import annotations

import sys
import time

import openai

from config.user_orchestrator import LLM_SETTINGS, require_setting


def _format_prompt(template: str, data: str) -> str:
	"""Insert the evidence text into the prompt template."""

	return template.replace("{data}", data)


class OpenAIResponder:
	"""Generate responses using the OpenAI API within a RAG workflow."""

	def __init__(self, data: str, model: str, prompt_template: str, client: openai.OpenAI) -> None:
		self.data = data
		self.model = model
		self.prompt_template = prompt_template
		self.client = client
		self.prompt = _format_prompt(prompt_template, data)

	def generate_response(self, retries: int = 1, backoff_seconds: float = 0.5) -> tuple[str, dict | None]:
		"""Get one response from the model and return text plus usage metadata."""

		last_error: Exception | None = None
		for attempt in range(retries + 1):
			try:
				request_kwargs: dict = {
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

				response = self.client.chat.completions.create(
					**request_kwargs,
				)

				if not response or not response.choices:
					raise RuntimeError("Empty response from LLM (no choices)")

				content = response.choices[0].message.content if response.choices[0].message else None
				if content is None:
					raise RuntimeError("Empty response from LLM (no message content)")

				text = content.strip()
				if not text:
					raise RuntimeError("Empty response from LLM (blank content)")

				usage = getattr(response, "usage", None)
				if usage is not None:
					usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else dict(usage)
				else:
					usage_dict = None
				return text, usage_dict
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