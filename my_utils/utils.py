import subprocess
import time
import os
import math
import re
import threading
import signal
from pathlib import Path

from .helpers import b, log

from .types import (
    Config,
    Variation,
    RunProperties,
    Metric,
)
from .constants import (
    BASE_DIR,
    MICROBENCH_DIR,
    PYTHON_DIR,
    TOOLS_DIR,
    EXPERIMENTS_DIR,
    PINNING,
    THREADS_PER_SOCKET,
    TOTAL_THREAD_COUNT,
    PMEM_DIRS,
    PLOT_PYTHON_DIR,
    PLOTTING_TOOLS_DIR
)


def process_cpu_stats():
    res = dict()
    sockets = b(f"{TOOLS_DIR / 'get_lscpu_numa_nodes.sh'}").stdout.strip().split("\n")
    for i, threads in enumerate(sockets):
        res[i] = dict()
        threads = threads.strip().split(" ")
        halfpoint = len(threads) // 2
        res[i]["threads"] = threads[:halfpoint]
        res[i]["hyperthreads"] = threads[halfpoint:]
    return res


SOCKET_STATS = process_cpu_stats()


def get_pinning_and_numa(thread_count, socket_ordering, prefer_hyperthreads):
    res = []
    sockets_used = set()
    for socket_idx in socket_ordering:
        # socket_idx = socket_idx % len(SOCKET_STATS) # to be compatible with pyke
        socket = SOCKET_STATS[socket_idx]
        threads = socket["threads"]
        hyperthreads = socket["hyperthreads"]

        for thread in threads:
            res.append(thread)
            sockets_used.add(socket_idx)
            if len(res) == thread_count:
                return ",".join(res), ",".join(map(str, list(sockets_used)))

        if prefer_hyperthreads:
            for hyperthread in hyperthreads:
                res.append(hyperthread)
                sockets_used.add(socket_idx)
                if len(res) == thread_count:
                    return ",".join(res), ",".join(map(str, list(sockets_used)))

    for socket_idx in socket_ordering:
        socket = SOCKET_STATS[socket_idx]
        threads = socket["threads"]
        hyperthreads = socket["hyperthreads"]

        for hyperthread in hyperthreads:
            res.append(hyperthread)
            sockets_used.add(socket_idx)
            if len(res) == thread_count:
                return ",".join(res), ",".join(map(str, list(sockets_used)))

    return ",".join(res), ",".join(map(str, list(sockets_used)))


def get_utilized_numa_nodes(thread_count):
    utilized = math.ceil(thread_count / THREADS_PER_SOCKET)
    return ",".join([str(i) for i in range(utilized)])


class Experiment:
    def __init__(self, config: Config, script_path: Path):
        time_string = str(int(time.time()))
        self.config = config
        self.script_path = script_path
        self.dir = EXPERIMENTS_DIR / f"{time_string}_{self.config.description}"
        self._make_dirs()

    def _make_dirs(self):
        log.info("Making experiment directories...")
        log.info(f"Experiment directory: {self.dir}")
        os.mkdir(self.dir)
        os.mkdir(f"{self.dir}/logs")
        os.mknod(f"{self.dir}/logs/perfLogs.log")
        os.mkdir(f"{self.dir}/texts")
        os.mkdir(f"{self.dir}/plot_texts")
        os.mkdir(f"{self.dir}/jsons")
        os.mkdir(f"{self.dir}/plots")
        os.mkdir(f"{self.dir}/perfs")
        os.mkdir(f"{self.dir}/flames")
        os.mkdir(f"{self.dir}/per_universe_plot_texts")
        os.mkdir(f"{self.dir}/per_universe_plots")
        os.mknod(f"{self.dir}/NOTES.md")

        if self.script_path is not None:
            b(f"cp {self.script_path} {self.dir}/reproduce_script.py")

    def _compile(self, variation: Variation):
        variation.compile(self.dir)

    def _handle_huge_pages(self, variation: Variation, reset=False):
        if not variation.huge_pages or reset:
            log.info("Setting hugepages to madvise")
            b("echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled")
        else:
            log.info("Setting hugepages to always")
            b("echo always | sudo tee /sys/kernel/mm/transparent_hugepage/enabled")

    def _cleanup(self):
        pass

    def _construct_run_command(
        self, rp: RunProperties, insertion_time_and_memory=False
    ):
        res = "timeout 1200 /usr/bin/time --verbose "

        working_thread_count = rp.thread_count.total_threads()
        if rp.variation.numa_kwargs is not None:
            if rp.variation.numa_kwargs["mode"] == "all":
                res += "numactl --interleave=all "
            else:
                numa = get_pinning_and_numa(
                    thread_count=working_thread_count,
                    socket_ordering=rp.variation.numa_kwargs["socket_ordering"],
                    prefer_hyperthreads=rp.variation.numa_kwargs["prefer_hyperthreads"],
                )[1]
                res += f"numactl --interleave={numa} "
        else:
            # if numa kwargs is none, don't use numactl at all
            pass

        if len(rp.variation.stat_events) > 0:
            res += f"perf stat -e {','.join(rp.variation.stat_events)} "

            # THE LABEL IS THE NEW NAME OF THE BINARY FILE
        # res += f"./bin/{rp.variation.label} "
        res += f"{rp.variation.executable} "

        res += f"-nprefill {rp.thread_count.nprefill} -nwork {rp.thread_count.nwork} -nrq {rp.thread_count.nrq} -rqsize {rp.thread_count.rqsize} -niter {rp.thread_count.niter} "
        # res += f"-i {rp.workload.i} -d {rp.workload.d} -rq 0 -rqsize 1 -nrq 0 -succ {rp.workload.succ} -pred {rp.workload.pred} "
        res += f"-i {rp.workload.i} -d {rp.workload.d} "

        max_key = int(rp.variation.prefill_count * rp.variation.universe_coefficient)
        log.info(
            f"Max key: {rp.variation.prefill_count} * {rp.variation.universe_coefficient} = {max_key}"
        )

        if not insertion_time_and_memory:
            prefill_size = rp.variation.prefill_count
        else:
            prefill_size = rp.prefill_size

        res += f"-k {max_key} -prefillsize {prefill_size} {rp.variation.prefill_mode} "

        if rp.variation.pin_kwargs is not None:
            pinning = get_pinning_and_numa(
                thread_count=working_thread_count,
                socket_ordering=rp.variation.pin_kwargs["socket_ordering"],
                prefer_hyperthreads=rp.variation.pin_kwargs["prefer_hyperthreads"],
            )[0]
            if working_thread_count <= TOTAL_THREAD_COUNT:
                res += f"-pin {pinning} "

        res += f"-t {rp.variation.millis} "

        if rp.variation.distribution == "zipf":
            res += f"-dist-zipf {rp.variation.alpha} "
        elif rp.variation.distribution == "zipf-fast":
            res += f"-dist-zipf-fast {rp.variation.alpha} "
        elif rp.variation.distribution == "parlay-uniform":
            res += "-dist-parlay-uniform "
        elif rp.variation.distribution == "parlay-zipf":
            res += f"-dist-parlay-zipf {rp.variation.alpha} "
        elif rp.variation.distribution == "zipf-ycsb":
            res += f"-dist-zipf-ycsb {rp.variation.alpha} "

        if rp.variation.additional_run_args != "":
            res += f"{rp.variation.additional_run_args} "

        log.info(res.strip())
        return res.strip()

    def _get_environ(self, variation: Variation):
        environ = os.environ.copy()

        preload_paths = []
        library_paths = []

        if hasattr(variation, "libraries"):
            for library in variation.libraries:
                cur_preload = ":".join([f"{opath}" for opath in library.preload_paths])
                preload_paths.append(cur_preload)

            library_paths = []
            for library in variation.libraries:
                cur_library = ":".join([f"{opath}" for opath in library.dashL_paths])
                library_paths.append(cur_library)

        environ["LD_LIBRARY_PATH"] = (
            ":".join(library_paths) + ":" + environ.get("LD_LIBRARY_PATH", "")
        )

        environ["LD_PRELOAD"] = ":".join(preload_paths)
        log.info(f"LD_PRELOAD: {environ.get('LD_PRELOAD', '')}")
        log.info(f"LD_LIBRARY_PATH: {environ.get('LD_LIBRARY_PATH', '')}")

        for key, value in variation.environ.items():
            log.info(f"Environ: {key}={value}")
            environ[key] = value

        return environ

    def _construct_metrics_dict(self, output_path):
        res = dict()

        with open(output_path, "r", encoding="utf-8", errors="replace") as fp:
            lines = fp.readlines()
            lines = [
                line.strip().replace("\n", "").replace("=", "=0")
                for line in lines
                if re.match("[A-Za-z0-9_]+=", line) is not None and line.count("=") == 1
            ]

        for line in lines:
            dirty_metric, value = line.split("=")
            res[dirty_metric] = value

        # with open(output_path, "r", encoding='utf-8', errors='replace') as fp:

        return res

    def _plot_metric_arrayline(
        self, metric: Metric, metrics_dict: dict, rp: RunProperties
    ):
        metric_value = metric.resolve(metrics_dict, rp)
        plotbars_script_path = f"{TOOLS_DIR / 'plotlines.py'}"

        text_path = f"{self.dir}/per_universe_plot_texts/{metric.label}-{rp.workload}-{rp.retry}-{rp.thread_count}.txt"
        plot_path = f"{self.dir}/per_universe_plots/{metric.label}-{rp.workload}-{rp.retry}-{rp.thread_count}.png"

        plot_cmd = f'cat {text_path} | {PLOT_PYTHON_DIR} {plotbars_script_path} --logy --lightmode --legend-include --legend-columns 10 -t "{metric.label}-{rp.variation.distribution}{rp.variation.alpha if rp.variation.distribution == "zipf" else ""}-prefillCount{rp.variation.prefill_count} ({rp.workload})" --x-title "X" --y-title "{metric.label}" --height 14 --width 38 -o "{plot_path}"'

        with open(text_path, "a") as fp:
            for i, value in enumerate(metric_value):
                fp.write(f"{rp.variation.label} {i} {value}\n")
            fp.flush()

        plot_thread = threading.Thread(target=b, args=(plot_cmd,))
        plot_thread.start()
        plot_thread.join()

    def _plot_metric_bar(self, metric: Metric, metrics_dict: dict, rp: RunProperties):
        metric_value = metric.resolve(metrics_dict, rp)
        plotbars_script_path = PLOTTING_TOOLS_DIR / 'plot.py'

        text_path = f"{self.dir}/plot_texts/{metric.label}-{rp.workload}.txt"
        plot_path = f"{self.dir}/plots/{metric.label}-{rp.workload}.png"
        repr_path = f"{self.dir}/plot_texts/{metric.label}-{rp.workload}-REPRODUCE.sh"
        plot_cmd = f'cat {text_path} | {PLOT_PYTHON_DIR} {plotbars_script_path} --lightmode --legend-include --legend-columns 12 -t "{metric.label}-{rp.variation.distribution}{rp.variation.alpha if rp.variation.distribution == "zipf" else ""}-prefillCount{rp.variation.prefill_count} ({rp.workload})" --x-title "Number of threads" --y-title "{metric.label}" --height 20 --width 28 -o "{plot_path}"'

        with open(text_path, "a") as fp:
            if rp.thread_count.query_threads() > 0:
                # x_text = f"{rp.thread_count.nwork + rp.thread_count.nrq} = {rp.thread_count.nwork}work + {rp.thread_count.nrq}rq(size={str(rp.thread_count.rqsize).zfill(9)})"
                x_text = f"rqsize={str(rp.thread_count.rqsize).zfill(9)}"
            else:
                x_text = f"{rp.thread_count.nwork}"
            fp.write(f'{rp.variation.label} "{x_text}" {metric_value}\n')
            fp.flush()

        with open(repr_path, "w") as fp:
            fp.write(plot_cmd)
            fp.flush()

        # add per-universe plots as well
        pu_text_path = (
            f"{self.dir}/per_universe_plot_texts/{metric.label}-{rp.workload}.txt"
        )
        pu_plot_path = f"{self.dir}/per_universe_plots/{metric.label}-{rp.workload}.png"
        pu_repr_path = (
            f"{self.dir}/per_universe_plot_texts/{metric.label}-{rp.workload}-REPRODUCE.sh"
        )
        pu_plot_cmd = f'cat {pu_text_path} | {PLOT_PYTHON_DIR} {plotbars_script_path} --lightmode --legend-include --legend-columns 4 -t "{metric.label}-{rp.variation.distribution}{rp.variation.alpha if rp.variation.distribution == "zipf" else ""}-prefillCount{rp.variation.prefill_count} ({rp.workload})" --x-title "Number of threads" --y-title "{metric.label}" --height 20 --width 28 -o "{pu_plot_path}"'

        with open(pu_text_path, "a") as fp:
            if rp.thread_count.query_threads() > 0:
                # x_text = f"{rp.thread_count.nwork + rp.thread_count.nrq} = {rp.thread_count.nwork}work + {rp.thread_count.nrq}rq(size={str(rp.thread_count.rqsize).zfill(6)})"
                x_text = f"rqsize={str(rp.thread_count.rqsize).zfill(9)}"
            else:
                x_text = f"{rp.thread_count.nwork}"
            fp.write(f'{rp.variation.label} "{x_text}" {metric_value}\n')
            fp.flush()

        with open(pu_repr_path, "w") as fp:
            fp.write(pu_plot_cmd)
            fp.flush()

        plotbars_script_path = PLOTTING_TOOLS_DIR / 'plot.py'

        plot_thread = threading.Thread(target=b, args=(plot_cmd,))
        pu_plot_thread = threading.Thread(target=b, args=(pu_plot_cmd,))

        plot_thread.start()
        pu_plot_thread.start()

        plot_thread.join()
        pu_plot_thread.join()
        
    def _plot_metric_stacked_breakdown(self, metric: Metric, metrics_dict: dict, rp: RunProperties):
        stacks = metric.resolve(metrics_dict, rp)
        plotbars_script_path = PLOTTING_TOOLS_DIR / 'plot.py'
        
        pu_text_path = (
            f"{self.dir}/per_universe_plot_texts/{metric.label}-{rp.workload}.txt"
        ) 
        pu_plot_path = f"{self.dir}/per_universe_plots/{metric.label}-{rp.workload}.png"
        pu_repr_path = (
            f"{self.dir}/per_universe_plot_texts/{metric.label}-{rp.workload}-REPRODUCE.sh"
        )
        
        pu_plot_cmd = f'cat {pu_text_path} | {PLOT_PYTHON_DIR} {plotbars_script_path} --lightmode --legend-include --legend-columns 8 -t "{metric.label}-{rp.variation.distribution}{rp.variation.alpha if rp.variation.distribution == "zipf" else ""}-prefillCount{rp.variation.prefill_count} ({rp.workload})" --x-title "Number of threads" --y-title "{metric.label}" --height 20 --width 28 -o "{pu_plot_path}" --stacked'
        
        with open(pu_text_path, "a") as fp: 
            # Each line looks like this: 
            # <part of stack> <x-label> <value>
            # Where x-label is <thread_count>_<ds_label>
            
            for stack_part, value in stacks.items():
                thread_text = f"{rp.thread_count.nwork}Threads"
                if rp.thread_count.query_threads() > 0:
                    thread_text = f"{rp.thread_count.nwork + rp.thread_count.nrq + rp.thread_count.niter} = {rp.thread_count.nwork}work + {rp.thread_count.nrq}rq(size={str(rp.thread_count.rqsize).zfill(6)}) + {rp.thread_count.niter}iter"
                ds_text = rp.variation.label
                
                x_text = f"{thread_text}_{ds_text}"
                fp.write(f'{stack_part} "{x_text}" {value}\n')
            fp.flush()
            
        with open(pu_repr_path, "w") as fp:
            fp.write(pu_plot_cmd)
            fp.flush()
            
        plot_thread = threading.Thread(target=b, args=(pu_plot_cmd,))
        plot_thread.start()
        plot_thread.join()
        
                
    

    def _plot_metric(self, metric: Metric, metrics_dict: dict, rp: RunProperties):
        if metric.plot_type == "bar":
            self._plot_metric_bar(metric, metrics_dict, rp)
        elif metric.plot_type == "arrayline":
            self._plot_metric_arrayline(metric, metrics_dict, rp)
        elif metric.plot_type == "stacked-breakdown":
            self._plot_metric_stacked_breakdown(metric, metrics_dict, rp)

    def _collect_results(self, rp: RunProperties):
        output_path = rp.get_output_path(self.dir)
        metrics_dict = self._construct_metrics_dict(output_path)

        threads = []

        for metric in self.config.metrics:
            if metric.label.startswith("Txn"):
                if rp.variation.txn:
                    threads.append(
                        threading.Thread(
                            target=self._plot_metric, args=(metric, metrics_dict, rp)
                        )
                    )
                else:
                    pass  # do nothing
            else:
                threads.append(
                    threading.Thread(
                        target=self._plot_metric, args=(metric, metrics_dict, rp)
                    )
                )

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()


    def _start_perf(self, rp: RunProperties, proc):
        log.info("Starting perf")
        fp = open(f"{self.dir}/logs/perfLogs.log", "a")
        return subprocess.Popen(
            [
                # "taskset",
                # "-c",
                # "142,143",
                "perf",
                "record",
                "-k",
                "CLOCK_MONOTONIC",
                "-F",
                "99",
                "--call-graph",
                "dwarf",
                "-o",
                f"{rp.get_perf_path(self.dir)}",
            ],
            stdout=fp,
            stderr=fp,
        )

    def _generate_flame(self, rp: RunProperties):
        output_path = rp.get_output_path(self.dir)
        metrics_dict = self._construct_metrics_dict(output_path)
        start = metrics_dict["REALTIME_START_PERF_FORMAT"][:-5]
        end = metrics_dict["REALTIME_END_PERF_FORMAT"][:-5]
        log.info(f"Start: {start}, End: {end}")
        perf_path = rp.get_perf_path(self.dir)
        flame_path = rp.get_flame_path(self.dir)
        log.info("Generating flamegraph...")
        b(
            f"perf script -i {perf_path} --time {start},{end} | ~/tools/stackcollapse-perf.pl | ~/tools/flamegraph.pl --inverted > {flame_path}"
        )

    def dry_run(self):
        total_secs = 0
        for workload in self.config.workloads:
            log.info(f"beginning experiments for workload {workload}")
            for thread_count in self.config.thread_counts:
                log.info(f"Thread count: {thread_count}")
                for variation in self.config.variations:
                    log.info(f"Variation: {variation.label}")
                    for retry in range(self.config.retries):
                        total_secs += variation.millis // 1000
        return total_secs

    # ad-hoc for artifact evaluation
    # def run_insertion_times_and_memory(self, keys, thread_num, universe):
    #     os.chdir(MICROBENCH_DIR)

    #     for variation in self.config.variations:
    #         self._compile(variation)

    #     workloads = [Workload(i=0, d=0)]
    #     for workload in workloads:
    #         for prefill_size in keys:
    #             for variation in self.config.variations:
    #                 rp = RunProperties(retry=0, workload=workload, thread_num=thread_num, variation=variation, prefill_size=prefill_size)
    #                 run_command = self._construct_run_command(rp, insertion_time_and_memory=True)
    #                 rp._replace(run_command=run_command)
    #                 environ = self._get_environ(variation)
    #                 fp = open(rp.get_output_path(self.dir), "w", encoding="utf-8")
    #                 experiment_process = subprocess.Popen(run_command.split(" "), stderr=fp, stdout=fp, env=environ)
    #                 experiment_process.wait()
    #                 if experiment_process.returncode != 0:
    #                     log.info(run_command)
    #                     log.error("ERRRORRRRRRR IN COMMAND ABOVE")
    #                     log.error(f"Return code: {experiment_process.returncode}")
    #                     # exit(0)
    #                 fp.flush()
    #                 fp.close()

    #                 if experiment_process.returncode == 0:
    #                     self._collect_results_for_insertion_time_and_memory(rp)

    def safe_to_collect(self, process, rp: RunProperties):
        if process.returncode == 0:
            return True
        output_path = rp.get_output_path(self.dir)
        output_content = open(output_path, "r").read()
        if "Structural validation OK." in output_content:
            return True
        return False

    def run(self, skip_compilation=False):
        os.chdir(MICROBENCH_DIR)

        if not skip_compilation:
            # compilation_threads = []
            # for variation in self.config.variations:
                # thread = threading.Thread(target=self._compile, args=(variation,))
                # compilation_threads.append(thread)
            # for thread in compilation_threads:
                # thread.start()
            # for thread in compilation_threads:
                # thread.join()
            
            # sequential compilation
            for variation in self.config.variations:
                self._compile(variation)
        else:
            for variation in self.config.variations:
                variation.executable = f"./bin/{variation.label}"

        for workload in self.config.workloads:
            log.info(f"beginning experiments for workload {workload}")
            for retry in range(self.config.retries): 
                for thread_count in self.config.thread_counts:
                    log.info(f"Thread count: {thread_count}")
                    for variation in self.config.variations:
                        log.info(f"Variation: {variation.label}")
                        log.info(f"Prefill count: {variation.prefill_count}")
                        log.info("Removing persistent memory...")
                        for path in PMEM_DIRS:
                            # log.info(f"Clearing path {path}")
                            res = b(f"rm -rf {path}/*")
                            if res.returncode != 0:
                                log.error(f"Error clearing path {path}")
                                log.error(res.stderr)
                                exit(0)

                        log.info(f"retry {retry}")
                        rp = RunProperties(
                            retry=retry,
                            workload=workload,
                            thread_count=thread_count,
                            variation=variation,
                        )
                        run_command = self._construct_run_command(rp)
                        rp._replace(run_command=run_command)

                        environ = self._get_environ(variation)

                        # self._handle_huge_pages(variation, reset=False)

                        fp = open(rp.get_output_path(self.dir), "w", encoding="utf-8")
                        experiment_process = subprocess.Popen(
                            run_command.split(" "),
                            stderr=fp,
                            stdout=fp,
                            env=environ,
                            text=True,
                        )

                        if variation.perf:
                            perf_process = self._start_perf(rp, experiment_process)

                        experiment_process.wait()

                        if experiment_process.returncode != 0:
                            log.info(run_command)
                            log.error("ERRRORRRRRRR IN COMMAND ABOVE")
                            log.error(f"Return code: {experiment_process.returncode}")
                            # exit(0)

                        if variation.perf:
                            perf_process.send_signal(signal.SIGINT)
                            perf_process.wait()
                            self._generate_flame(rp)

                        # self._handle_huge_pages(variation, reset=True)

                        fp.flush()
                        fp.close()

                        if self.safe_to_collect(experiment_process, rp):
                            self._collect_results(rp)
                            
                        log.info(f"Output path is {rp.get_output_path(self.dir)}")

        self._cleanup()
