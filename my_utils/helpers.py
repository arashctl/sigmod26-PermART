import subprocess
import os
import logging
import sys


# also set up logging to call command "telelog X" where the X is the message
class TelelogHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        b(f'{os.environ.get("DOTFILES")}/scripts/telelog/telelog.sh "{log_entry}"')


# SET UP THE LOGGING
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


log = logging.getLogger("PMEM")


def r(
    command_list,
    stderr=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    environ=os.environ.copy(),
):
    proc = subprocess.run(command_list, stderr=stderr, stdout=stdout, env=environ)
    return proc


# bash command
def b(command):
    proc = subprocess.run(command, shell=True, capture_output=True, text=True)
    return proc
