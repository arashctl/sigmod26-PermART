from .helpers import b
from pathlib import Path 


BASE_DIR = Path(__file__).resolve().parent.parent
PYTHON_DIR  = BASE_DIR / "venv" / "bin" / "python3"
PLOTTING_TOOLS_DIR = BASE_DIR / "plotting-tools"
PLOT_PYTHON_DIR = PLOTTING_TOOLS_DIR / "venv" / "bin" / "python3"
MICROBENCH_DIR = BASE_DIR / "microbench"
CMAKE_BUILD_DIR = BASE_DIR / ".experimental_cmake_builds"
TOOLS_DIR = BASE_DIR / "tools"
EXPERIMENTS_DIR = BASE_DIR / ".my_experiments"
PINNING = b(f"{TOOLS_DIR / 'get_pinning_cluster.sh'}").stdout.strip()
# PINNING = "18-35,90-107,36-53,108-125,54-71,126-143,0-17,72-89"
THREADS_PER_SOCKET = 2 * int(
    b('lscpu | grep "Core(s) per socket" | grep -E [0-9]+ -o').stdout.strip()
)
TOTAL_THREAD_COUNT = (
    2
    * int(b('lscpu | grep "Core(s) per socket" | grep -E [0-9]+ -o').stdout.strip())
    * int(b('lscpu | grep "Socket(s)" | grep -E [0-9]+ -o').stdout.strip())
)
PMEM_DIRS = [
    Path("/mnt/pmem1_mount/mkhalaji/mimalloc_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/mimalloc_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/ralloc_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/ralloc_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/lvmlc_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/lvmlc_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/viper_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/viper_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/pmemobj_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/pmemobj_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/pactree_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/pactree_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/roart_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/roart_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/lbtree_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/lbtree_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/dptree_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/dptree_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/utree_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/utree_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/apex_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/apex_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/rntree_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/rntree_playground"),
    Path("/mnt/pmem0_mount/mkhalaji/bztree_playground"),
    Path("/mnt/pmem1_mount/mkhalaji/bztree_playground"),
]