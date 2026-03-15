from my_utils.types import Library
from pathlib import Path 


setbench_jemalloc = Library(
    label="jemalloc", 
    preload_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/lib/libjemalloc.so")],
)

ralloc = Library(
    label="ralloc", 
    dashI_paths=[Path("/home/mkhalaji/pmem_playground/ralloc/src/")], 
    dashL_paths=[Path("/home/mkhalaji/pmem_playground/ralloc/test/")], 
    linker_flags=["-lralloc"]
)

ralloc_numa0 = Library(
    label="ralloc_numa0", 
    dashI_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_numa0/ralloc/src/")], 
    dashL_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_numa0/ralloc/test/")],
    linker_flags=["-lrallocnuma0"]
)

ralloc_numa1 = Library(
    label="ralloc_numa1", 
    dashI_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_numa1/ralloc/src/")], 
    dashL_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_numa1/ralloc/test/")],
    linker_flags=["-lrallocnuma1"]
)

# ralloc_n0dl = Library(
#     label="ralloc_n0dl", 
#     dashI_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src")], 
#     dashL_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/")],
#     linker_flags=["-lrallocn0dl"]
# )

# ralloc_n1dl = Library(
#     label="ralloc_n1dl", 
#     dashI_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src")], 
#     dashL_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/")],
#     linker_flags=["-lrallocn1dl"]
# )


ralloc_n0dl = Library(
    label="ralloc_n0dl", 
    dashI_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/src/")], 
    dashL_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n0dl/test/")],
    linker_flags=["-lrallocn0dl"]
)

ralloc_n1dl = Library(
    label="ralloc_n1dl", 
    dashI_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/src/")], 
    dashL_paths=[Path("/home/mkhalaji/pmem_playground/ralloc_for_pactree/n1dl/test/")],
    linker_flags=["-lrallocn1dl"]
)


my_mimalloc = Library(
    label="mimalloc",
    dashI_paths=[Path("/home/mkhalaji/repos/mimalloc/include/")],
    linker_flags=["-lmimalloc"]
)

vmmalloc_no_override = Library(
    label="vmmalloc", 
    dashI_paths=[Path("/home/mkhalaji/repos/vmemBranches/no-override/vmem/src/include/")],
    dashL_paths=[Path("/home/mkhalaji/repos/vmemBranches/no-override/vmem/src/nondebug/")], 
    preload_paths=[Path("/home/mkhalaji/repos/vmemBranches/no-override/vmem/src/nondebug/libvmmalloc.so")],
    linker_flags=["-lvmmalloc"]
)

vmmalloc_yes_override = Library(
    label="vmmalloc", 
    dashI_paths=[Path("/home/mkhalaji/repos/vmemBranches/master/vmem/src/include/")],
    dashL_paths=[Path("/home/mkhalaji/repos/vmemBranches/master/vmem/src/nondebug/")], 
    preload_paths=[Path("/home/mkhalaji/repos/vmemBranches/master/vmem/src/nondebug/libvmmalloc.so")],
    linker_flags=["-lvmmalloc"]
)

fptree_library = Library(
    label="fptree", 
    dashI_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/07-fptree/")], 
    dashL_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/07-fptree/")],
    linker_flags=["-lFPTree"], 
    preload_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/07-fptree/libFPTree.so")]
)

libpmem = Library(
    label="libpmem",
    dashI_paths=["/usr/include"], 
    linker_flags=["-lpmem"], 
    dashL_paths=["/usr/lib/x86_64-linux-gnu/"]
)

libpmemobj = Library(
    label="libpmemobj",
    dashI_paths=["/usr/include"],
    linker_flags=["-lpmemobj"],
    dashL_paths=["/usr/lib/x86_64-linux-gnu/"]
)

my_tbb = Library(
    label="custom_tbb", 
    dashI_paths=[Path("/home/mkhalaji/repos/oneTBB/include/")],
    dashL_paths=[Path("/home/mkhalaji/repos/oneTBB/build/gnu_9.4_cxx11_64_release/")],
    linker_flags=["-ltbbmod"] # IMPORTANT TO RENAME THE COMPILED LIBRARY TO libtbbmod.so TO AVOID CONFLICTS
)

tbb = Library(
    label="tbb", 
    dashI_paths=[Path("/usr/include")],
    dashL_paths=[Path("/lib/x86_64-linux-gnu")],
    linker_flags=["-ltbb"]
)

rntree_tbb = Library(
    label="rntree_tbb",
    linker_flags=["-ltbb"], 
    dashL_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/14-rntree/")],
    dashI_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/14-rntree/tbb/")]
)

pdlart = Library(
    label="pdlart", 
    linker_flags=["-lpdlart"],
    dashL_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/09-pactree/")], 
    dashI_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/09-pactree/lib/PDL-ART/include/")],
)

pactree = Library(
    label="pactree", 
    linker_flags=["-lpactree"],
    dashL_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/09-pactree/")], 
    dashI_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/09-pactree/include/"), Path("/home/mkhalaji/wbb/setbench_verlib/ds/09-pactree/src/")],
)

liblbtree_pmem = Library(
    label="liblbtree",
    linker_flags=["-llbtree_pmem"],
    dashL_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/11-lbtree/")],
    dashI_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/11-lbtree/"),],
)

apex_pmdk = Library(
    label="apex_pmdk",
    linker_flags=["-lpmem", "-lpmemobj"], 
    dashL_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/13-apex/") / "pmdk" / "src" / "PMDK" / "src" / "nondebug"],
    dashI_paths=[Path("/home/mkhalaji/wbb/setbench_verlib/ds/13-apex/") / "pmdk" / "src" / "PMDK" / "src" / "include"  
                 ],
)


pmwcas_root = Path("/home/mkhalaji/repos/pmwcas")
pmwcas = Library(
    label="pmwcas", 
    linker_flags=["-lpmwcas"],
    dashI_paths=[
        pmwcas_root, 
        pmwcas_root / "include",
        # pmwcas_root/ "src"
    ], 
    dashL_paths=[pmwcas_root / "build"]
)

glog = Library(
    label="glog", 
    linker_flags=["-lglog"],
    dashI_paths=[
        pmwcas_root / "build" / "_deps" / "glog-build", 
        pmwcas_root / "build" / "_deps" / "glog-src" / "src"
    ], 
    dashL_paths=[pmwcas_root / "build" / "_deps" / "glog-build"]
)