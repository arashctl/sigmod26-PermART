from .types import *
from .libraries import *




ABTree = MakeVariation(
    label="abtree",
    libraries=[ralloc_n0dl,ralloc_n1dl],
    compile_xargs="-DUSE_RALLOC -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread", 
    compile_flags="march_native=1 no_optimize=0 use_tree_stats=0 ALLOCATORS=ralloc",
    binary="24-abtree-pm-occ.ralloc.debra.pnone",
    perf=False, 
    millis=6000,
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
)


#   
#    ███████████ ███████████  ███████████                            
#   ░░███░░░░░░█░░███░░░░░███░█░░░███░░░█                            
#    ░███   █ ░  ░███    ░███░   ░███  ░  ████████   ██████   ██████ 
#    ░███████    ░██████████     ░███    ░░███░░███ ███░░███ ███░░███
#    ░███░░░█    ░███░░░░░░      ░███     ░███ ░░░ ░███████ ░███████ 
#    ░███  ░     ░███            ░███     ░███     ░███░░░  ░███░░░  
#    █████       █████           █████    █████    ░░██████ ░░██████ 
#   ░░░░░       ░░░░░           ░░░░░    ░░░░░      ░░░░░░   ░░░░░░  
#   
#   

FPTreeBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/07-fptree")
FPTreeBase = MakeVariation(
    label="fptree", 
    libraries=[my_tbb],
    millis=6000,
    binary="07-fptree.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=1", 
    compile_xargs="-DUseSetbenchTids -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution='zipf-ycsb',
    additional_source_files=[
        FPTreeBaseDir / "fptree.cpp"
    ]
)


FPTreeDRAM = FPTreeBase.replace(
    compile_xargs=FPTreeBase.compile_xargs,
)

FPTreePMEM = FPTreeBase.replace(
    libraries=[libpmem, libpmemobj, my_tbb],
    compile_xargs=FPTreeBase.compile_xargs + " -DPMEM",
)



#   
#    ███████████      ███████      █████████   ███████████   ███████████
#   ░░███░░░░░███   ███░░░░░███   ███░░░░░███ ░░███░░░░░███ ░█░░░███░░░█
#    ░███    ░███  ███     ░░███ ░███    ░███  ░███    ░███ ░   ░███  ░ 
#    ░██████████  ░███      ░███ ░███████████  ░██████████      ░███    
#    ░███░░░░░███ ░███      ░███ ░███░░░░░███  ░███░░░░░███     ░███    
#    ░███    ░███ ░░███     ███  ░███    ░███  ░███    ░███     ░███    
#    █████   █████ ░░░███████░   █████   █████ █████   █████    █████   
#   ░░░░░   ░░░░░    ░░░░░░░    ░░░░░   ░░░░░ ░░░░░   ░░░░░    ░░░░░    
#   

ROARTBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/10-roart")
ROARTIncludes = f"-I{ROARTBaseDir}/ART -I{ROARTBaseDir}/nvm_mgr"
ROARTBase = MakeVariation(
    label="roart", 
    libraries=[libpmem, libpmemobj, tbb, setbench_jemalloc], 
    millis=6000,
    binary="10-roart.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=0", 
    compile_xargs=f"{ROARTIncludes} -DUseSetbenchTids -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution="zipf-ycsb",
    environ={
    }, 
    additional_source_files=[
        ROARTBaseDir / "ART" / "LeafArray.cpp",
        ROARTBaseDir / "ART" / "N.cpp",
        ROARTBaseDir / "ART" / "N4.cpp",
        ROARTBaseDir / "ART" / "N16.cpp",
        ROARTBaseDir / "ART" / "N48.cpp",
        ROARTBaseDir / "ART" / "N256.cpp",
        ROARTBaseDir / "ART" / "Tree.cpp",
        ROARTBaseDir / "nvm_mgr" / "Epoch.cpp",
        ROARTBaseDir / "nvm_mgr" / "nvm_mgr.cpp",
        ROARTBaseDir / "nvm_mgr" / "threadinfo.cpp",
    ]
)

ROARTPMEM = ROARTBase
ROARTPMDK = ROARTBase.replace(
    compile_xargs=ROARTBase.compile_xargs + " -DARTPMDK",
)
ROARTDRAM = ROARTBase.replace(
    compile_xargs=ROARTBase.compile_xargs + " -DDRAM_MODE",
)



#   
#    █████       ███████████   █████                               
#   ░░███       ░░███░░░░░███ ░░███                                
#    ░███        ░███    ░███ ███████   ████████   ██████   ██████ 
#    ░███        ░██████████ ░░░███░   ░░███░░███ ███░░███ ███░░███
#    ░███        ░███░░░░░███  ░███     ░███ ░░░ ░███████ ░███████ 
#    ░███      █ ░███    ░███  ░███ ███ ░███     ░███░░░  ░███░░░  
#    ███████████ ███████████   ░░█████  █████    ░░██████ ░░██████ 
#   ░░░░░░░░░░░ ░░░░░░░░░░░     ░░░░░  ░░░░░      ░░░░░░   ░░░░░░  
#
   
LBTreeBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/11-lbtree")
LBTreeAdditionalSourceFiles = [
    LBTreeBaseDir / f"{x}.cc" for x in ["lbtree", "mempool", "nvm-common", "tree"] 
]
LBTreeBase = MakeVariation(
    label="lbtree", 
    libraries=[libpmem, libpmemobj, tbb, setbench_jemalloc],
    millis=6000,
    binary="11-lbtree.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=0",
    compile_xargs="-DUseSetbenchTids -pthread -mrtm -msse4.1 -mavx2 -Wall",
    perf=False,
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution="zipf-ycsb",
    additional_source_files=LBTreeAdditionalSourceFiles, 
    environ={}, 
)

LBTreeDRAM = LBTreeBase
LBTreePMEM = LBTreeBase.replace(
    label="lbtree-pmdk",
    compile_xargs=LBTreeBase.compile_xargs + " -DPMEM",
)
LBTreeRalloc = LBTreeBase.replace(
    label="lbtree-ralloc",
    libraries=[tbb, ralloc_n0dl, ralloc_n1dl],
    compile_xargs=LBTreeBase.compile_xargs + " -DPMEM -DUSE_RALLOC",
)

#    
#     ███████████    █████████     █████████  ███████████                            
#    ░░███░░░░░███  ███░░░░░███   ███░░░░░███░█░░░███░░░█                            
#     ░███    ░███ ░███    ░███  ███     ░░░ ░   ░███  ░  ████████   ██████   ██████ 
#     ░██████████  ░███████████ ░███             ░███    ░░███░░███ ███░░███ ███░░███
#     ░███░░░░░░   ░███░░░░░███ ░███             ░███     ░███ ░░░ ░███████ ░███████ 
#     ░███         ░███    ░███ ░░███     ███    ░███     ░███     ░███░░░  ░███░░░  
#     █████        █████   █████ ░░█████████     █████    █████    ░░██████ ░░██████ 
#    ░░░░░        ░░░░░   ░░░░░   ░░░░░░░░░     ░░░░░    ░░░░░      ░░░░░░   ░░░░░░  
#   

# NOLIB

PACTreeBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/09-pactree-nolib")
PACTreeIncludeDirs = [PACTreeBaseDir / "include", PACTreeBaseDir / "src", PACTreeBaseDir / "lib" / "PDL-ART"]
PACTreeIncludeDirs = [PACTreeBaseDir / "include", PACTreeBaseDir / "src"]

PACTreeIncludes = " ".join([f"-I{x}" for x in PACTreeIncludeDirs])
PACTreeAdditionalSourceFiles = [
    PACTreeBaseDir / "src" / f"{x}.cpp" 
    for x in [
        "linkedList", "listNode", "pactree", "WorkerThread", "Oplog"
    ]
] + [
    PACTreeBaseDir / "lib" / "PDL-ART" / f"{x}.cpp"
    for x in [
        "Tree"
    ] # N4, N16, N48, N256 are included in N.cpp... sigh. N.cpp is included in Tree.cpp... SIGH. So is Epoche.cpp...
]

PACTreeBase = MakeVariation(
    label="pactree", 
    libraries=[libpmem, libpmemobj, tbb, setbench_jemalloc],
    millis=6000,
    binary="09-pactree-nolib.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=1", 
    compile_xargs=f"{PACTreeIncludes} -DVALUE_TYPE=uint64_t -DUseSetbenchTids -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution="zipf-ycsb",
    additional_source_files=PACTreeAdditionalSourceFiles,
    environ={
        "PACTREE_SOCKET0_ROOT": "/mnt/pmem0_mount/mkhalaji/pactree_playground", 
        "PACTREE_SOCKET1_ROOT": "/mnt/pmem1_mount/mkhalaji/pactree_playground"
    }
) 


#     ██████████   ███████████  ███████████                            
#    ░░███░░░░███ ░░███░░░░░███░█░░░███░░░█                            
#     ░███   ░░███ ░███    ░███░   ░███  ░  ████████   ██████   ██████ 
#     ░███    ░███ ░██████████     ░███    ░░███░░███ ███░░███ ███░░███
#     ░███    ░███ ░███░░░░░░      ░███     ░███ ░░░ ░███████ ░███████ 
#     ░███    ███  ░███            ░███     ░███     ░███░░░  ░███░░░  
#     ██████████   █████           █████    █████    ░░██████ ░░██████ 
#    ░░░░░░░░░░   ░░░░░           ░░░░░    ░░░░░      ░░░░░░   ░░░░░░  

DPTreeBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/08-dptree")
DPTreeAdditionalSourceFiles = [
    DPTreeBaseDir / "src" / "art_idx.cpp", 
    DPTreeBaseDir / "src" / "ART.cpp",
    DPTreeBaseDir / "src" / "bloom.c",
    DPTreeBaseDir / "src" / "MurmurHash2.cpp",
    DPTreeBaseDir / "src" / "util.cpp",
]
DPTreeBase = MakeVariation(
    label="dptree",
    libraries=[libpmem, libpmemobj, tbb, setbench_jemalloc],
    millis=6000,
    binary="08-dptree.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=0",
    compile_xargs="-mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread -msse -msse2 -DHAS_AVX512",
    perf=False,
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution="zipf-ycsb",
    additional_source_files=DPTreeAdditionalSourceFiles,
    environ={}
)

DPTreeDRAM = DPTreeBase
DPTreePMEM = DPTreeBase.replace(
    compile_xargs=DPTreeBase.compile_xargs + " -DPMEM"
)


#             ███████████                            
#            ░█░░░███░░░█                            
#  █████ ████░   ░███  ░  ████████   ██████   ██████ 
# ░░███ ░███     ░███    ░░███░░███ ███░░███ ███░░███
#  ░███ ░███     ░███     ░███ ░░░ ░███████ ░███████ 
#  ░███ ░███     ░███     ░███     ░███░░░  ░███░░░  
#  ░░████████    █████    █████    ░░██████ ░░██████ 
#   ░░░░░░░░    ░░░░░    ░░░░░      ░░░░░░   ░░░░░░  

uTreeBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/12-utree")
uTreeBase = MakeVariation(
    label="utree", 
    libraries=[libpmem, libpmemobj, setbench_jemalloc],
    millis=6000,
    binary="12-utree.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=0", 
    compile_xargs="-DUseSetbenchTids -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -pthread", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution='zipf-ycsb',
    additional_source_files=[
    ]
)

uTreeDRAM = uTreeBase
uTreePMEM = uTreeBase.replace(
    compile_xargs=uTreeBase.compile_xargs + " -DPMEM",
)

#   
#    █████   █████  ███                              
#   ░░███   ░░███  ░░░                               
#    ░███    ░███  ████  ████████   ██████  ████████ 
#    ░███    ░███ ░░███ ░░███░░███ ███░░███░░███░░███
#    ░░███   ███   ░███  ░███ ░███░███████  ░███ ░░░ 
#     ░░░█████░    ░███  ░███ ░███░███░░░   ░███     
#       ░░███      █████ ░███████ ░░██████  █████    
#        ░░░      ░░░░░  ░███░░░   ░░░░░░  ░░░░░     
#                        ░███                        
#                        █████                       
#                       ░░░░░                  
#       




#    █████████   ███████████  ██████████ █████ █████
#   ███░░░░░███ ░░███░░░░░███░░███░░░░░█░░███ ░░███ 
#  ░███    ░███  ░███    ░███ ░███  █ ░  ░░███ ███  
#  ░███████████  ░██████████  ░██████     ░░█████   
#  ░███░░░░░███  ░███░░░░░░   ░███░░█      ███░███  
#  ░███    ░███  ░███         ░███ ░   █  ███ ░░███ 
#  █████   █████ █████        ██████████ █████ █████
# ░░░░░   ░░░░░ ░░░░░        ░░░░░░░░░░ ░░░░░ ░░░░░ 

APEXBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/13-apex")
APEXEpochInclude = f"-I{APEXBaseDir}/epoch_reclaimer-src"
APEXBase = MakeVariation(
    label="apex", 
    libraries=[apex_pmdk, setbench_jemalloc, tbb],
    millis=6000,
    binary="13-apex.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=1 use_tree_stats=0", 
    compile_xargs=f"{APEXEpochInclude} -DUseSetbenchTids -lnuma -lrt -ldl -mrtm -msse4.1 -mavx2 -lpthread", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution='zipf-ycsb',
    additional_source_files=[
    ]
)

APEXPMEM = APEXBase.replace(
    compile_xargs=APEXBase.compile_xargs + " -DPMEM -DPMDK",
)

#  ███████████   ██████   █████ ███████████                            
# ░░███░░░░░███ ░░██████ ░░███ ░█░░░███░░░█                            
#  ░███    ░███  ░███░███ ░███ ░   ░███  ░  ████████   ██████   ██████ 
#  ░██████████   ░███░░███░███     ░███    ░░███░░███ ███░░███ ███░░███
#  ░███░░░░░███  ░███ ░░██████     ░███     ░███ ░░░ ░███████ ░███████ 
#  ░███    ░███  ░███  ░░█████     ░███     ░███     ░███░░░  ░███░░░  
#  █████   █████ █████  ░░█████    █████    █████    ░░██████ ░░██████ 
# ░░░░░   ░░░░░ ░░░░░    ░░░░░    ░░░░░    ░░░░░      ░░░░░░   ░░░░░░  


RNTreeBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/14-rntree")
RNTreeBase = MakeVariation(
    label="rntree", 
    libraries=[rntree_tbb], 
    millis=6000,
    binary="14-rntree.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=0", 
    compile_xargs=f"-DUseSetbenchTids -lnuma -lrt -ldl -mrtm -msse4.1 -mavx2 -lpthread", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution='zipf-ycsb',
    additional_source_files=[
        RNTreeBaseDir / "threadinfo.cpp",
        RNTreeBaseDir / "nvm_mgr.cpp",
    ]
)

RNTreePMEM = RNTreeBase.replace(
    compile_xargs=RNTreeBase.compile_xargs + " -DUSE_NVM_MALLOC -DCLEAR_NVM_POOL",
)

#  ███████████             ███████████                            
# ░░███░░░░░███           ░█░░░███░░░█                            
#  ░███    ░███  █████████░   ░███  ░  ████████   ██████   ██████ 
#  ░██████████  ░█░░░░███     ░███    ░░███░░███ ███░░███ ███░░███
#  ░███░░░░░███ ░   ███░      ░███     ░███ ░░░ ░███████ ░███████ 
#  ░███    ░███   ███░   █    ░███     ░███     ░███░░░  ░███░░░  
#  ███████████   █████████    █████    █████    ░░██████ ░░██████ 
# ░░░░░░░░░░░   ░░░░░░░░░    ░░░░░    ░░░░░      ░░░░░░   ░░░░░░  

BzTreeBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/15-bztree")
BzTreePMEM = MakeVariation(
    label="bztree",
    libraries=[libpmem, libpmemobj, glog, pmwcas],
    millis=6000, 
    binary="15-bztree.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=0 use_tree_stats=0", 
    compile_xargs=f"-DUseSetbenchTids -lnuma -lrt -ldl -mrtm -msse4.1 -mavx2 -lpthread -pthread -DPMEM_BACKEND=PMDK -DPMEM -DPMDK -DDESC_CAP=16 -DMAX_FREEZE_RETRY=1 -DENABLE_MERGE=0", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution='zipf-ycsb',
    additional_source_files=[
        BzTreeBaseDir / "bztree.cc"
    ]
)

#  ███████████                    █████                ███████████            ███           
# ░░███░░░░░░█                   ░░███         ███    ░░███░░░░░░█           ░░░            
#  ░███   █ ░   ██████    █████  ███████      ░███     ░███   █ ░   ██████   ████  ████████ 
#  ░███████    ░░░░░███  ███░░  ░░░███░    ███████████ ░███████    ░░░░░███ ░░███ ░░███░░███
#  ░███░░░█     ███████ ░░█████   ░███    ░░░░░███░░░  ░███░░░█     ███████  ░███  ░███ ░░░ 
#  ░███  ░     ███░░███  ░░░░███  ░███ ███    ░███     ░███  ░     ███░░███  ░███  ░███     
#  █████      ░░████████ ██████   ░░█████     ░░░      █████      ░░████████ █████ █████    
# ░░░░░        ░░░░░░░░ ░░░░░░     ░░░░░              ░░░░░        ░░░░░░░░ ░░░░░ ░░░░░     

FastFairBaseDir = Path("/home/mkhalaji/wbb/setbench_verlib/ds/16-fastfair")
FastFairPMEM = MakeVariation(
    label="fastfair",
    libraries=[libpmem, libpmemobj],
    millis=6000, 
    binary="16-fastfair.new.debra.pnone",
    huge_pages=False,
    prefill_count=0,
    pin_kwargs=dict(socket_ordering=[1, 0], prefer_hyperthreads=True),
    numa_kwargs=dict(mode="func", socket_ordering=[1, 0], prefer_hyperthreads=True),
    compile_flags=f"march_native=1 no_optimize=1 use_tree_stats=0", 
    compile_xargs=f"-DUseSetbenchTids -lnuma -lrt -ldl -mrtm -msse4.1 -mavx2 -lpthread -pthread -lm", 
    perf=False, 
    alpha=0.99,
    prefill_mode="-prefill-insert",
    distribution='zipf-ycsb',
    additional_source_files=[
    ]
)