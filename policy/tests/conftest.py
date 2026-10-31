import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # ss_policy
sys.path.insert(0, str(Path(__file__).resolve().parent))  # fixtures

# The Phase 6 DR scenarios kill whole regions and need a fresh `up.sh --dr` stack: they only run through dr_run.sh (SS_DR_TESTS=1).
import os

collect_ignore = [] if os.environ.get("SS_DR_TESTS") else ["test_dr.py", "test_dr_failback.py", "test_dr_stale_c.py"]
