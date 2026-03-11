"""Direct run: python backup_to_github.py"""

import subprocess
import sys
from datetime import datetime

BACKUP_MSG = f"Automated backup after pipeline run on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

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

    def run_backup(self) -> None:
        """human readable hint: execute pull, add, commit, and push in safe sequence."""

        self.run_command(["git", "pull"])
        self.run_command(["git", "add", "."])
        self.run_command(["git", "commit", "-m", self.backup_message])
        self.run_command(["git", "push"])
        print("Backup complete! Your changes are now on GitHub.")


def run(cmd: list[str]) -> None:
    """Compatibility wrapper for older calls."""

    BackupToGitHub(BACKUP_MSG).run_command(cmd)

def main():
    # human readable hint: pulling first reduces push conflicts when multiple users work on the repo.
    BackupToGitHub(BACKUP_MSG).run_backup()

if __name__ == "__main__":
    main()
