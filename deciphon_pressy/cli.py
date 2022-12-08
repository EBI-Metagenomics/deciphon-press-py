import asyncio
from pathlib import Path

import typer

from deciphon_pressy.pressy import create_pressy

app = typer.Typer()


@app.command()
def server():
    pass


@app.command()
def press(hmm_file: Path):
    async def main():
        async with create_pressy(hmm_file.name) as pressy:
            await pressy.wait()
        raise typer.Exit(pressy.result.value)

    asyncio.run(main())
