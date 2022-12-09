import asyncio
from pathlib import Path

import requests
from kombu import Connection, Exchange, Queue

from deciphon_pressy.pressy import create_pressy
from deciphon_pressy.requests import upload

# from deciphon_pressy.work import single_press, start_server


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


async def amain(hmm_file: Path):
    async with create_pressy(hmm_file.name) as pressy:
        await pressy.wait()
    # raise typer.Exit(pressy.result.value)


def process_hmm(hmm, message):
    try:
        print(hmm)
        hmm_id = hmm["id"]
        filename = hmm["filename"]
        download(f"http://127.0.0.1:8000/hmms/{hmm_id}/download", filename)
        print("Download finished")
        hmm_file = Path(filename).resolve()

        asyncio.run(amain(hmm_file))

        mime = "application/octet-stream"
        db_file = filename.replace(".hmm", ".dcp")
        upload("http://127.0.0.1:8000/dbs/", "db_file", Path(db_file), mime)
        message.ack()
    except Exception as e:
        print(e)


def create_server():
    hmm_exchange = Exchange("hmm", "direct", durable=True)
    hmm_queue = Queue("hmm", exchange=hmm_exchange, routing_key="hmm")
    with Connection("amqp://guest:guest@localhost//") as conn:
        with conn.Consumer(hmm_queue, callbacks=[process_hmm]) as consumer:
            while True:
                conn.drain_events()
