from pathlib import Path
import sys
import io

PROJECT_DIR = Path(__file__).parent.parent
LOG_DIR = PROJECT_DIR / ".log"
modules = [str(m) for m in PROJECT_DIR.glob("WM_*") if not m.name.endswith("backup")]

for m in modules:
    sys.path.insert(0, m)
