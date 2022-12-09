import asyncio
from pathlib import Path

import typer

from deciphon_pressy.pressy import create_pressy
from deciphon_pressy.server import create_server

app = typer.Typer()


@app.command()
def server():
    # async def main():
    create_server()
    # async with create_server() as srv:
    #     await srv.wait()
    # raise typer.Exit(srv.result.value)

    # asyncio.run(main())


@app.command()
def press(hmm_file: Path):
    async def main():
        async with create_pressy(hmm_file.name) as pressy:
            await pressy.wait()
        raise typer.Exit(pressy.result.value)

    asyncio.run(main())
