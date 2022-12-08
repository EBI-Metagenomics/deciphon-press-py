from functools import partial
from pathlib import Path
from signal import SIGINT, SIGTERM
from subprocess import PIPE

import trio

from deciphon_pressy.line_reader import LineReader
from deciphon_pressy.pressy_file import pressy_file


async def tickle(proc: trio.Process, send_ch, task_status=trio.TASK_STATUS_IGNORED):
    assert proc.stdin
    async with send_ch:
        with trio.CancelScope() as scope:
            task_status.started(scope)
            while True:
                await send_ch.send("query")
                await trio.sleep(1)
    print("exiting tickle")


async def input_handler(proc: trio.Process, recv_ch):
    assert proc.stdin
    async with recv_ch:
        async for msg in recv_ch:

            if msg.startswith("press"):
                hmm_file = msg.split(" ", 1)[1]
                await proc.stdin.send_all(f"press {hmm_file}\n".encode())

            elif msg.startswith("quit"):
                await proc.stdin.send_all("quit\n".encode())
                break

            elif msg.startswith("query"):
                await proc.stdin.send_all("state | {1} {2} {3}\n".encode())

    print("exiting input handler")


async def output_handler(proc: trio.Process, send_ch):
    assert proc.stdout
    reader = LineReader(proc.stdout)
    async with send_ch:
        while True:
            try:
                line = (await reader.readline()).strip()
            except trio.BrokenResourceError:
                print("BrokenResourceError")
                break
            if line == "":
                print("EOF")
                break
            result, state, progress = line.split(" ", 3)
            if result == "ok" and state in ["done", "fail", "run"]:
                await send_ch.send(" ".join([state, progress]))
    print("exiting output handler")


async def signal_handler(send_ch, task_status=trio.TASK_STATUS_IGNORED):
    async with send_ch:
        with trio.CancelScope() as scope:
            with trio.open_signal_receiver(SIGTERM, SIGINT) as it:
                task_status.started(scope)
                async for signum in it:
                    if signum == SIGTERM:
                        await send_ch.send("sigterm")
                        break
                    if signum == SIGINT:
                        await send_ch.send("sigint")
                        break
    print("exiting signal handler")


async def boss(pressy: str, workdir: Path, hmm_file: str):
    run_proc = partial(trio.run_process, [pressy], stdin=PIPE, stdout=PIPE, cwd=workdir)

    async with trio.open_nursery() as nursery:
        task_send_ch, task_recv_ch = trio.open_memory_channel(0)
        watch_send_ch, watch_recv_ch = trio.open_memory_channel(0)

        async with task_send_ch, task_recv_ch, watch_send_ch, watch_recv_ch:

            proc = await nursery.start(run_proc)
            assert isinstance(proc, trio.Process)
            nursery.start_soon(partial(input_handler, proc, task_recv_ch))
            nursery.start_soon(partial(output_handler, proc, watch_send_ch.clone()))
            sighdl = await nursery.start(partial(signal_handler, watch_send_ch.clone()))

            await task_send_ch.send(f"press {hmm_file}")
            monitor = await nursery.start(partial(tickle, proc, task_send_ch.clone()))

            async for msg in watch_recv_ch:
                print(f"Boss recv: {msg}")

                if msg == "sigint" or msg == "sigterm":
                    monitor.cancel()
                    sighdl.cancel()
                    await task_send_ch.send("quit")
                    break

                if msg.startswith("done"):
                    monitor.cancel()
                    sighdl.cancel()
                    await task_send_ch.send("quit")
                    break
                if msg.startswith("fail"):
                    print("fail")
                if msg.startswith("run"):
                    print("run")

                if msg.startswith("finished"):
                    return

            await proc.wait()


async def single_press(workdir: Path, hmm_file):
    with pressy_file() as pressy:
        await boss(str(pressy), workdir, hmm_file)


async def start_server():
    with pressy_file() as pressy:
        await boss(str(pressy))
