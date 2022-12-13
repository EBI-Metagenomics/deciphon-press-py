import threading
from functools import lru_cache
from subprocess import PIPE, Popen

import requests
from kombu import Connection, Exchange, Queue
from pydantic import BaseSettings
from requests_toolbelt.multipart import encoder
from requests_toolbelt.multipart.encoder import MultipartEncoderMonitor

BIN = "/Users/horta/code/deciphon-pressy/deciphon_pressy/pressy-mac"


class Config(BaseSettings):
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    api_prefix: str = ""
    api_key: str = "change-me"
    verbose: bool = False

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        validate_assignment = True

    @property
    def api_url(self):
        return f"http://{self.api_host}:{self.api_port}{self.api_prefix}"


@lru_cache
def get_config() -> Config:
    return Config()


config = get_config()


def fire_and_forget(f):
    def wrapped():
        threading.Thread(target=f).start()

    return wrapped


def patch(path: str):
    hdrs = {
        "Accept": "application/json",
        "X-API-KEY": config.api_key,
    }
    try:
        return requests.patch(url(path), headers=hdrs)
    except ConnectionError as conn_error:
        print(conn_error)


def press_hmm(hmm_file: str, job_id: int):
    proc = Popen([BIN, hmm_file], stdout=PIPE)
    assert proc.stdout
    last_progress = 0
    for raw_line in proc.stdout:
        line = raw_line.decode().strip()
        if line == "done":
            print(line)
        elif line == "fail":
            print(line)
        else:
            cur_progress = int(line.replace("%", ""))
            if cur_progress > last_progress:
                inc = cur_progress - last_progress

                @fire_and_forget
                def send_update():
                    patch(f"/jobs/{job_id}/progress/increment/{inc}")

                send_update()
                last_progress = cur_progress
                print(cur_progress)

    exit_code = proc.wait()
    print(f"exitcode: {exit_code}")


chunk_size = 64 * 1024


def url(path: str) -> str:
    return f"{config.api_url}{path}"


def download(path: str, filename: str):
    hdrs = {
        "Accept": "*/*",
        "X-API-KEY": config.api_key,
    }
    with requests.get(url(path), stream=True, headers=hdrs) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                # filter out keep-alive new chunks
                if chunk:
                    f.write(chunk)


class UploadProgress:
    def __init__(self, total_bytes: int, filename: str):
        # self._bar = tqdm_file(total_bytes, filename)
        self._bytes_read = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        del args
        # self._bar.close()

    def __call__(self, monitor: MultipartEncoderMonitor):
        increment = monitor.bytes_read - self._bytes_read
        # self._bar.update(increment)
        self._bytes_read += increment


def upload(path: str, field_name: str, filepath: str, mime: str) -> str:
    e = encoder.MultipartEncoder(
        fields={
            field_name: (
                filepath,
                open(filepath, "rb"),
                mime,
            )
        }
    )
    with UploadProgress(e.len, filepath) as up:
        monitor = encoder.MultipartEncoderMonitor(e, up)
        hdrs = {
            "Accept": "application/json",
            "Content-Type": monitor.content_type,
            "X-API-KEY": config.api_key,
        }
        r = requests.post(
            url(path),
            data=monitor,  # type: ignore
            headers=hdrs,
        )
        # r.raise_for_status()
    return r.json()
    # return pretty_json(r.json())


def process_request(hmm, message):
    try:
        print(hmm)
        hmm_id = hmm["id"]
        hmm_file = hmm["filename"]
        job_id = hmm["job_id"]

        patch(f"/jobs/{job_id}/set-run")

        download(f"/hmms/{hmm_id}/download", hmm_file)
        print("Download finished")
        press_hmm(hmm_file, job_id)

        mime = "application/octet-stream"
        db_file = hmm_file.replace(".hmm", ".dcp")
        upload("/dbs/", "db_file", db_file, mime)
        message.ack()
    except Exception as e:
        print(e)
        message.ack()


def create_server():
    hmm_exchange = Exchange("hmm", "direct", durable=True)
    hmm_queue = Queue("hmm", exchange=hmm_exchange, routing_key="hmm")
    with Connection("amqp://guest:guest@localhost//") as conn:
        with conn.Consumer(hmm_queue, callbacks=[process_request]) as consumer:
            while True:
                conn.drain_events()


create_server()
