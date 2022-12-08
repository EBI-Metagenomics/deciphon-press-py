from functools import partial
from pathlib import Path

import trio
import typer

from deciphon_pressy.work import single_press

app = typer.Typer()


@app.command()
def server():
    typer.echo("Serving")


@app.command()
def press(hmm_file: Path):
    trio.run(partial(single_press, hmm_file.parent, hmm_file.name))
