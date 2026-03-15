from typing import List, NamedTuple, Callable
from pathlib import Path
from dataclasses import dataclass, field, replace
from .helpers import r, b, log
import os
from .constants import BASE_DIR, CMAKE_BUILD_DIR, MICROBENCH_DIR
import copy # for deepcopy


class Workload(NamedTuple):
    i: int = 0
    d: int = 0
    rq: int = 0

    def __str__(self):
        return (
            f"i{str(self.i).zfill(2)}-d{str(self.d).zfill(2)}-rq{str(self.rq).zfill(2)}"
        )


class Library(NamedTuple):
    label: str
    dashI_paths: List[Path] = []
    dashL_paths: List[Path] = []
    preload_paths: List[Path] = []
    linker_flags: List[str] = []


@dataclass
class Variation:
    label: str
    millis: int
    huge_pages: bool
    prefill_count: int
    pin_kwargs: dict
    numa_kwargs: dict
    universe_coefficient: int = 2
    executable: Path = None
    additional_run_args: str = ""
    prefill_mode: str = "-prefill-insert"
    distribution: str = "uniform"
    stat_events: List[str] = field(default_factory=list)
    alpha: float = 0.99
    perf: bool = False
    txn: bool = False
    environ: dict = field(default_factory=dict)
    has_libpapi: int = 0

    def replace(self, **kwargs):
        return replace(copy.deepcopy(self), **kwargs)

    def __str__(self):
        return f"""
        Variation
        label:                  {self.label}
        millis:                 {self.millis}
        prefill_count:          {self.prefill_count}
        additional_run_args:    {self.additional_run_args}
        distribution:           {self.distribution}
        alpha:                  {self.alpha}
    """


@dataclass
class MakeVariation(Variation):
    binary: str = "btree"
    compile_flags: str = ""
    additional_source_files: List[Path] = field(default_factory=list)
    compile_xargs: str = None
    libraries: List[Library] = field(default_factory=list)

    def compile(self, experiment_dir: Path):
        curdir = os.curdir
        os.chdir(MICROBENCH_DIR)
        b(f"rm -rf bin/{self.label}")

        log.info(f"Compiling microbench for {self.label}")

        fp = open(
            f"{experiment_dir}/logs/compile-{self.label}.log", "w", encoding="utf-8"
        )

        command = f"make has_libpapi={self.has_libpapi} DATA_STRUCTURES='{self.binary.split('.')[0]}' -j all"
        # command = f"make has_libpapi={self.config.has_libpapi} -j all"

        if self.compile_flags != "" and self.compile_flags is not None:
            command += f" {self.compile_flags}"

        # add the libraries to compile command
        arglist = []
        for library in self.libraries:
            arglist += [" ".join([f"-I{ipath}" for ipath in library.dashI_paths])]
            arglist += [" ".join([f"-L{opath}" for opath in library.dashL_paths])]
            arglist += [" ".join([f"{flag}" for flag in library.linker_flags])]
        libargs = " ".join(arglist)

        additional_source_args = " ".join(
            [f"{path}" for path in self.additional_source_files]
        )

        log.info(
            f"{command} xargs='{self.compile_xargs}' libargs='{libargs}' ADDITIONAL_SOURCE_FILES='{additional_source_args}'"
        )

        environ = os.environ.copy()
        if self.compile_xargs is not None:
            environ["xargs"] = self.compile_xargs
        if len(libargs) > 0:
            environ["libargs"] = libargs

        if len(additional_source_args) > 0:
            log.info(f"Additional source files: {additional_source_args}")
            environ["ADDITIONAL_SOURCE_FILES"] = additional_source_args

        proc = r(command.split(" "), stdout=fp, stderr=fp, environ=environ)

        fp.flush()
        fp.close()

        if proc.returncode == 0:
            log.info("Compilation successful")
        else:
            log.fatal("Compilataion failed")
            log.fatal(f"Return code: {proc.returncode}")
            log.fatal(proc.stdout)
            log.fatal(proc.stderr)
            exit(-1)

        log.info("Renaming the compiled binary")
        b(f"rm -rf bin/{self.label}")
        b(f"mv bin/{self.binary} bin/{self.label}")

        self.executable = MICROBENCH_DIR / f"bin/{self.label}"

        os.chdir(curdir)


@dataclass
class CMakeVariation(Variation):
    ds_name: str = "btree"
    target: str = "all"
    cmake_args: List[str] = field(default_factory=list)
    
    
    def with_allocator(self, allocator: str):
        
        # first, make a copy of self
        _new = copy.deepcopy(self)

        for i, arg in enumerate(_new.cmake_args):
            if arg.startswith("-DALLOCATOR="):
                _new.cmake_args[i] = f"-DALLOCATOR={allocator}"
                return _new
            
        # not found, append
        _new.cmake_args.append(f"-DALLOCATOR={allocator}")
        return _new
    
    def with_additional_compile_def(self, arg: str):
        
        # first, make a copy of self
        _new = copy.deepcopy(self)
        
        for i, existing_arg in enumerate(_new.cmake_args):
            if existing_arg.startswith("-DADDITIONAL_DEFS="): 
                # append to the existing definitions
                existing_defs = existing_arg.split("=")[1]
                new_defs = existing_defs + ";" + arg
                _new.cmake_args[i] = f"-DADDITIONAL_DEFS={new_defs}"
                return _new
                
                
        _new.cmake_args.append(f"-DADDITIONAL_DEFS={arg}")

        return _new

    def compile(self, experiment_dir: Path):
        curdir = os.curdir
        os.chdir(MICROBENCH_DIR / CMAKE_BUILD_DIR)
        # make a directory for this variation, with a combination of experiment name and variation label
        build_dir = Path(f"{experiment_dir.name}-build-{self.label}")
        b(f"mkdir -p {build_dir}")
        
        log.info(f"Compiling microbench for {self.label}")
        
        fp = open(
            f"{experiment_dir}/logs/compile-{self.label}.log", "w", encoding="utf-8"
        )
        
        cmake_command = "cmake " + " ".join(self.cmake_args) + f" -Dhas_libpapi={self.has_libpapi} -B {build_dir} -S {BASE_DIR}"
        
        log.info(f"Running cmake command: {cmake_command}")
        proc = r(cmake_command.split(" "), stdout=fp, stderr=fp)
        
        if proc.returncode != 0:
            log.fatal("CMake configuration failed")
            log.fatal(f"Return code: {proc.returncode}")
            log.fatal(proc.stdout)
            log.fatal(proc.stderr)
            exit(-1)
        log.info("CMake configuration successful, starting build")
        
        
        make_command = f"cmake --build {build_dir} --target {self.target}"
        
        proc = r(make_command.split(" "), stdout=fp, stderr=fp)
        
        fp.flush()
        fp.close()
        
        if proc.returncode == 0:
            log.info("Compilation successful")
            
        else: 
            log.fatal("Compilataion failed")
            log.fatal(f"Return code: {proc.returncode}")
            log.fatal(proc.stdout)
            log.fatal(proc.stderr)
            exit(-1)
        
        self.executable = build_dir / "ds" / self.ds_name / self.target
        log.info(f"Compiled binary is at {self.executable}")
            

        os.chdir(curdir)


class ThreadCount(NamedTuple):
    nprefill: int
    nwork: int
    nrq: int = 0
    rqsize: int = 0
    niter: int = 0

    def __str__(self):
        return f"nprefill{self.nprefill}-nwork{self.nwork}-nrq{self.nrq}(size={self.rqsize})-niter{self.niter}"
    
    def total_threads(self):
        return self.nprefill + self.nwork + self.nrq
    
    def working_threads(self):
        return self.nwork
    
    def query_threads(self):
        return self.nrq + self.niter


class RunProperties(NamedTuple):
    retry: int
    variation: Variation
    workload: Workload
    thread_count: ThreadCount
    prefill_size: int = 0  # Ad-hoc for AE. Do not trust what's in there. Only used for insertion time and memory plots.
    run_command: str = ""

    def __str__(self):
        return f"{self.variation.label}-{self.workload}-{self.thread_count}-retry{self.retry}"

    def get_output_path(self, dir):
        return f"{dir}/texts/{self}.txt"

    def get_perf_path(self, dir):
        return f"{dir}/perfs/{self}.perf.data"

    def get_flame_path(self, dir):
        return f"{dir}/flames/{self}.svg"


class Metric:
    def __init__(
        self, label: str, key: str, resolver: Callable, plot_type: str = "bar", **kwargs
    ):
        self.label = label
        self.key = key
        self.resolver = resolver
        self.plot_type = plot_type
        self.kwargs = kwargs

    def resolve(self, metrics_dic: dict, rp: RunProperties):
        return self.resolver(metrics_dic, rp, self.key, **self.kwargs)


class Config(NamedTuple):
    description: str
    workloads: List[Workload]
    metrics: List[Metric]
    thread_counts: List[ThreadCount]
    variations: List[Variation]
    retries: int = 1
