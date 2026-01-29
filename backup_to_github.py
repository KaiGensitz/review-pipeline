import subprocess
import sys
from datetime import datetime

BACKUP_MSG = f"Automated backup after pipeline run on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

def run(cmd):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"Command failed: {cmd}")
        sys.exit(result.returncode)

def main():
    run("git add .")
    run(f'git commit -m "{BACKUP_MSG}"')
    run("git pull")
    run("git push")
    print("Backup complete! Your changes are now on GitHub.")

if __name__ == "__main__":
    main()
