"""Back up the current pipeline state to a new GitHub branch.

Direct run:
    python backup_to_github_second_branch.py

Use this when you want to preserve the current working version on a separate
branch without committing it to the branch you started from. The script creates
a new branch from the current checkout, stages non-ignored files, commits them,
and pushes that new branch to GitHub.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime


DEFAULT_BRANCH_NAME = "pipeline-current-version-backup"


def _timestamp_label() -> str:
    """human readable hint: create a filename/branch-safe timestamp for uniqueness."""

    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _default_backup_message() -> str:
    """human readable hint: build a commit message that explains this is a branch backup."""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"Branch backup of current pipeline version on {timestamp}"


def _sanitize_branch_name(raw_name: str) -> str:
    """human readable hint: keep branch names simple so GitHub accepts them reliably."""

    cleaned = "".join(
        ch if (ch.isalnum() or ch in {"-", "_", "/"}) else "-"
        for ch in str(raw_name or "").strip()
    )
    while "//" in cleaned:
        cleaned = cleaned.replace("//", "/")
    cleaned = cleaned.strip("/-_")
    return cleaned or DEFAULT_BRANCH_NAME


def prompt_text(prompt: str, default_value: str) -> str:
    """human readable hint: ask for a value interactively and fall back cleanly otherwise."""

    # human readable hint: non-interactive terminals cannot answer prompts, so use the default.
    if not sys.stdin.isatty():
        print(f"Non-interactive terminal detected. Using default: {default_value}")
        return default_value

    print(prompt)
    print("Press Enter to use the default shown below.")
    print(f"Default: {default_value}")
    try:
        user_input = input("> ").strip()
    except EOFError:
        print("No input received. Using default.")
        return default_value
    return user_input if user_input else default_value


class GitCommandRunner:
    """human readable hint: one object owns Git command execution and error handling."""

    def run_command(self, cmd: list[str]) -> None:
        """human readable hint: run a Git command that must succeed."""

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            print(f"Command failed: {' '.join(cmd)}")
            sys.exit(result.returncode)

    def capture_command(self, cmd: list[str]) -> str:
        """human readable hint: run a Git command and return trimmed stdout."""

        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Command failed: {' '.join(cmd)}")
            if result.stderr:
                print(result.stderr.strip())
            sys.exit(result.returncode)
        return result.stdout.strip()

    def command_succeeds(self, cmd: list[str]) -> bool:
        """human readable hint: probe Git state without printing failure noise."""

        result = subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return result.returncode == 0

    def has_staged_changes(self) -> bool:
        """human readable hint: detect whether the branch backup needs a new commit."""

        result = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False)
        if result.returncode == 0:
            return False
        if result.returncode == 1:
            return True
        print("Command failed: git diff --cached --quiet")
        sys.exit(result.returncode)
        return False


class SecondBranchBackupToGitHub:
    """human readable hint: create, commit, and push a separate branch backup."""

    def __init__(
        self,
        *,
        target_branch: str,
        backup_message: str,
        runner: GitCommandRunner | None = None,
    ) -> None:
        """human readable hint: store the branch name, commit message, and Git runner."""

        self.target_branch = _sanitize_branch_name(target_branch)
        self.backup_message = backup_message
        self.runner = runner or GitCommandRunner()

    def current_branch(self) -> str:
        """human readable hint: remember where the backup branch was created from."""

        return self.runner.capture_command(["git", "branch", "--show-current"]) or "detached-head"

    def local_branch_exists(self, branch_name: str) -> bool:
        """human readable hint: avoid overwriting an existing local branch name."""

        return self.runner.command_succeeds(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch_name}"]
        )

    def unique_branch_name(self) -> str:
        """human readable hint: create a fresh branch name if the requested one already exists."""

        if not self.local_branch_exists(self.target_branch):
            return self.target_branch
        unique_name = f"{self.target_branch}-{_timestamp_label()}"
        print(
            f"Local branch '{self.target_branch}' already exists. "
            f"Using fresh branch '{unique_name}' instead."
        )
        return unique_name

    def run_backup(self) -> None:
        """human readable hint: create a new branch from the current files and push it."""

        source_branch = self.current_branch()
        target_branch = self.unique_branch_name()

        print(f"Source branch: {source_branch}")
        print(f"Backup branch: {target_branch}")
        print("This script changes Git branch metadata, but it does not rewrite file contents.")

        # human readable hint: creating a new branch preserves the current worktree exactly as it is.
        self.runner.run_command(["git", "switch", "-c", target_branch])

        # human readable hint: .gitignore still controls local-only folders such as input/ and output/.
        self.runner.run_command(["git", "add", "-A"])

        # human readable hint: when there are no uncommitted changes, push the branch pointer anyway.
        if self.runner.has_staged_changes():
            self.runner.run_command(["git", "commit", "-m", self.backup_message])
        else:
            print("No staged changes detected. The new branch will point to the current commit.")

        # human readable hint: -u stores the GitHub upstream so later pushes can use plain `git push`.
        self.runner.run_command(["git", "push", "-u", "origin", target_branch])
        print(f"Branch backup complete. GitHub branch: {target_branch}")
        print(f"To return later to the original branch, run: git switch {source_branch}")


def main() -> None:
    """human readable hint: command-line entrypoint for second-branch backup."""

    branch_name = prompt_text(
        "Enter the new Git branch name for this backup.",
        DEFAULT_BRANCH_NAME,
    )
    commit_message = prompt_text(
        "Enter commit message for this branch backup commit.",
        _default_backup_message(),
    )
    SecondBranchBackupToGitHub(
        target_branch=branch_name,
        backup_message=commit_message,
    ).run_backup()


if __name__ == "__main__":
    main()
