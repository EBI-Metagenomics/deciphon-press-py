import asyncio
import sys
from asyncio import create_subprocess_exec
from asyncio.subprocess import DEVNULL, PIPE
from contextlib import asynccontextmanager
from enum import Enum

from tqdm.asyncio import tqdm

from deciphon_pressy.pressy_file import pressy_file


class State(Enum):
    INIT = 0
    RUN = 1
    DONE = 2
    FAIL = 3
    QUIT = 4


class Result(Enum):
    SUCCEED = 0
    TIMEDOUT = 1
    CANCELLED = 2
    FAILED = 3


class Pressy:
    def __init__(self, proc, hmm_file: str, pbar: tqdm, no_stderr: bool):
        self._proc = proc
        self._hmm_file = hmm_file
        self._state: State = State.INIT
        self._result: Result = Result.FAILED
        self._pbar = pbar
        self._no_stderr = no_stderr

    @property
    def result(self):
        return self._result

    async def write_stdin(self, stdin):
        while self._state != State.QUIT:

            if self._state == State.INIT:
                self._state = State.RUN
                stdin.write((f"press {self._hmm_file}" + " | {1} _ _\n").encode())
                await stdin.drain()

            elif self._state == State.RUN:
                stdin.write(b"state | {1} {2} {3}\n")
                await stdin.drain()
                await asyncio.sleep(1)

            elif self._state == State.DONE or self._state == State.FAIL:
                self._state = State.QUIT
                stdin.write(b"quit\n")
                try:
                    await stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    pass

    async def read_stdout(self, stdout):
        while self._state != State.QUIT:

            buf = await stdout.readline()
            if not buf:
                break

            line = buf.decode().strip()
            result, state, progress = line.split(" ", 2)
            if result == "fail":
                self._state = State.FAIL
                self._result = Result.FAILED
            elif result == "ok" and state == "run":
                cur_perc = int(progress.replace("%", ""))
                self._pbar.update(cur_perc - self._pbar.n)

            if self._state == State.INIT:
                if state == "run" or state == "done":
                    self._state = State.RUN

            elif self._state == State.RUN:
                if state == "done":
                    self._state = State.DONE
                    self._result = Result.SUCCEED
                    self._pbar.update(100 - self._pbar.n)

                if state == "fail":
                    self._state = State.FAIL
                    self._result = Result.FAILED

    async def read_stderr(self, stderr):
        if self._no_stderr:
            return
        while self._state != State.QUIT:
            buf = await stderr.readline()
            if not buf:
                break
            print(buf.decode().strip(), file=sys.stderr)

    async def wait(self):
        await asyncio.gather(
            self.read_stderr(self._proc.stderr),
            self.read_stdout(self._proc.stdout),
            self.write_stdin(self._proc.stdin),
        )


@asynccontextmanager
async def create_pressy(hmm_file):
    try:
        with pressy_file() as file:
            proc = await create_subprocess_exec(
                file, "--loglevel=3", stdin=PIPE, stdout=PIPE, stderr=PIPE
            )
            with tqdm(total=100) as pbar:
                yield Pressy(proc, hmm_file, pbar, False)
    finally:
        await proc.wait()
