from functools import partial
from pathlib import Path

import requests
import trio
import typer
from kombu import Connection, Exchange, Queue

from deciphon_pressy.requests import upload
from deciphon_pressy.work import single_press, start_server

app = typer.Typer()


def download(url, filename):
    filename = url.split("/")[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    return filename


def process_hmm(hmm, message):
    print(hmm)
    hmm_id = hmm["id"]
    filename = hmm["filename"]
    download(f"http://127.0.0.1:8000/hmms/{hmm_id}/download", filename)
    print("Download finished")
    hmm_file = Path(filename).resolve()
    trio.run(partial(single_press, hmm_file.parent, hmm_file.name))
    mime = "application/octet-stream"
    db_file = filename.replace(".hmm", ".dcp")
    upload("http://127.0.0.1:8000/dbs/", "db_file", Path(db_file), mime)
    message.ack()


@app.command()
def server():
    hmm_exchange = Exchange("hmm", "direct", durable=True)
    hmm_queue = Queue("hmm", exchange=hmm_exchange, routing_key="hmm")
    with Connection("amqp://guest:guest@localhost//") as conn:
        with conn.Consumer(hmm_queue, callbacks=[process_hmm]) as consumer:
            while True:
                conn.drain_events()


@app.command()
def press(hmm_file: Path):
    trio.run(partial(single_press, hmm_file.parent, hmm_file.name))
