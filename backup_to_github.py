"""Back up the currently checked-out Git branch to GitHub.

Direct run:
    python backup_to_github.py

This script is intentionally small and explicit. It backs up the branch you are
already on by staging tracked/untracked non-ignored files, creating one commit,
and pushing that commit to the branch's configured upstream.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime


def _default_backup_message() -> str:
    """human readable hint: build a timestamped default commit message at runtime."""

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"Automated backup after pipeline run on {timestamp}"


def prompt_commit_message(default_message: str) -> str:
    """human readable hint: ask for a commit message but keep non-interactive runs safe."""

    # human readable hint: scheduled/non-interactive runs cannot answer prompts, so use the default.
    if not sys.stdin.isatty():
        print(f"Non-interactive terminal detected. Using default commit message: {default_message}")
        return default_message

    # human readable hint: an empty answer is not an error; it means "use the suggested message".
    print("Enter commit message for this backup commit.")
    print("Press Enter to use the default shown below.")
    print(f"Default: {default_message}")
    try:
        user_input = input("> ").strip()
    except EOFError:
        print("No input received. Using default commit message.")
        return default_message

    return user_input if user_input else default_message


class GitCommandRunner:
    """human readable hint: one small object owns all subprocess calls to Git."""

    def run_command(self, cmd: list[str]) -> None:
        """human readable hint: run one Git command and stop immediately if it fails."""

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            print(f"Command failed: {' '.join(cmd)}")
            sys.exit(result.returncode)

    def has_staged_changes(self) -> bool:
        """human readable hint: return True only when `git add` staged something commit-worthy."""

        result = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False)
        if result.returncode == 0:
            return False
        if result.returncode == 1:
            return True
        print("Command failed: git diff --cached --quiet")
        sys.exit(result.returncode)
        return False


class BackupToGitHub:
    """human readable hint: current-branch backup workflow with one public run method."""

    def __init__(self, backup_message: str, runner: GitCommandRunner | None = None) -> None:
        """human readable hint: store the commit message and injectable Git command runner."""

        self.backup_message = backup_message
        self.runner = runner or GitCommandRunner()

    def run_backup(self) -> None:
        """human readable hint: update branch, commit local changes, and push to GitHub."""

        # human readable hint: fast-forward only avoids accidental merge commits during a backup.
        self.runner.run_command(["git", "pull", "--ff-only"])

        # human readable hint: .gitignore decides what stays local; `git add -A` captures all allowed changes.
        self.runner.run_command(["git", "add", "-A"])

        # human readable hint: if nothing changed, there is no commit to make and no push is needed.
        if not self.runner.has_staged_changes():
            print("No staged changes detected. Skipping commit and push.")
            return

        # human readable hint: one backup run produces one Git commit for traceable rollback.
        self.runner.run_command(["git", "commit", "-m", self.backup_message])
        self.runner.run_command(["git", "push"])
        print("Backup complete. Your current branch changes are now on GitHub.")


def run(cmd: list[str]) -> None:
    """human readable hint: compatibility wrapper for older code that imported `run`."""

    GitCommandRunner().run_command(cmd)


def main() -> None:
    """human readable hint: command-line entrypoint for current-branch backup."""

    default_message = _default_backup_message()
    commit_message = prompt_commit_message(default_message)
    BackupToGitHub(commit_message).run_backup()


if __name__ == "__main__":
    main()
