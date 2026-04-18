"""Direct run: `python backup_to_github.py`"""

import subprocess
import sys
from datetime import datetime


def _default_backup_message() -> str:
    """human readable hint: build a timestamped default commit message at runtime."""

    return f"Automated backup after pipeline run on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"


def prompt_commit_message(default_message: str) -> str:
    """human readable hint: ask user for commit message and fall back to default when needed."""

    if not sys.stdin.isatty():
        print(f"Non-interactive terminal detected. Using default commit message: {default_message}")
        return default_message

    print("Enter commit message for this backup commit.")
    print("Press Enter to use the default shown below.")
    print(f"Default: {default_message}")
    try:
        user_input = input("> ").strip()
    except EOFError:
        print("No input received. Using default commit message.")
        return default_message

    return user_input if user_input else default_message

class BackupToGitHub:
    """human readable hint: one-class backup workflow with explicit command methods and one run entrypoint."""

    def __init__(self, backup_message: str) -> None:
        """human readable hint: __init__ stores the commit message used for the backup commit."""

        self.backup_message = backup_message

    def run_command(self, cmd: list[str]) -> None:
        """human readable hint: run one git command and stop the script when the command fails."""

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            print(f"Command failed: {' '.join(cmd)}")
            sys.exit(result.returncode)

    def has_staged_changes(self) -> bool:
        """human readable hint: detect whether there is anything staged before committing."""

        result = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False)
        if result.returncode == 0:
            return False
        if result.returncode == 1:
            return True
        print("Command failed: git diff --cached --quiet")
        sys.exit(result.returncode)

    def run_backup(self) -> None:
        """human readable hint: execute pull, add, commit, and push in safe sequence."""

        self.run_command(["git", "pull", "--ff-only"])
        self.run_command(["git", "add", "-A"])
        if not self.has_staged_changes():
            print("No staged changes detected. Skipping commit and push.")
            return
        self.run_command(["git", "commit", "-m", self.backup_message])
        self.run_command(["git", "push"])
        print("Backup complete! Your changes are now on GitHub.")


def run(cmd: list[str]) -> None:
    """Compatibility wrapper for older calls."""

    BackupToGitHub(_default_backup_message()).run_command(cmd)


def main():
    # human readable hint: pulling first reduces push conflicts when multiple users work on the repo.
    default_message = _default_backup_message()
    commit_message = prompt_commit_message(default_message)
    BackupToGitHub(commit_message).run_backup()

if __name__ == "__main__":
    main()
