from pathlib import Path
import sys
import io

PROJECT_DIR = Path(__file__).parent.parent
modules = [str(m) for m in PROJECT_DIR.glob("WM_*")]

for m in modules:
    if m.endswith("WM_backup"):
        modules.remove(m)

for m in modules:
    print("Loading:", m)
    sys.path.insert(0, m)
