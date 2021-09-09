import argparse
import asyncio
import math
import time
from asyncio.locks import Semaphore
from typing import Tuple, TypedDict

import aiohttp
import aiosqlite
import toml
import uvloop
from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError
from aiosqlite.core import Connection
from async_timeout import timeout
from rapidjson import dumps, loads

uvloop.install()


LIVE_USER_API = "http://api.live.bilibili.com/live_user/v1/Master/info"
# param: uid=x
ROOM_INIT_API = "http://api.live.bilibili.com/room/v1/Room/room_init"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Referer": "https://live.bilibili.com/",
}


class RoomInitError(Exception):
    def __init__(self, code: int, msg: str) -> None:
        self._code = code
        self._msg = msg
        super().__init__(msg)

    @property
    def code(self):
        return self._code

    @property
    def msg(self):
        return self._msg

    def __str__(self) -> str:
        return f"[RoomInit] code: {self._code}, message: {self._msg}"


class LiveUserError(Exception):
    def __init__(self, code: int, msg: str) -> None:
        self._code = code
        self._msg = msg
        super().__init__(msg)

    @property
    def code(self):
        return self._code

    @property
    def msg(self):
        return self._msg

    def __str__(self) -> str:
        return f"[LiveUser] code: {self._code}, message: {self._msg}"


class Proxy:
    def __init__(self, host: str, port: str) -> None:
        self._host = host
        self._port = port
        self._url = f"http://{host}:{port}"
        self._score = 3

    @property
    def url(self):
        return self._url

    @property
    def score(self):
        return self._score

    def __lt__(self, other):
        return self.score > other.score

    def punish_timeout(self):
        self._score -= 1

    def punish_connectError(self):
        self._score -= 2

    def discard(self):
        self._score -= 3

    def usable(self):
        return self._score > 0

    def __str__(self) -> str:
        return f"[{self._host}:{self._port}]"


class Medal(TypedDict):
    room_id: int
    short_id: int
    uid: int
    medal_name: str


class Job:
    def __init__(self, rid: int) -> None:
        self._rid = rid
        self._limit = 3

    @property
    def rid(self) -> int:
        return self._rid

    @property
    def exhausted(self) -> bool:
        return self._limit <= 0

    def fail(self):
        self._limit -= 1

    def __lt__(self, other):
        return self.rid < other.rid


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
) -> dict:
    async with session.get(url) as resp:
        json = await resp.json(loads=loads)
        # print(json)
        return json
        # return {"code": 0, "data": [{"host": "127.0.0.1", "port": "8080"}]}


async def fetch_proxy(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, str],
    proxy: Proxy,
) -> dict:
    print(params)
    async with session.get(url, params=params, proxy=proxy.url) as resp:
        # async with session.get(url, params=params) as resp:
        json = await resp.json(loads=loads)
        # print(json)
        return json


async def get_room_info(
    session: aiohttp.ClientSession, rid: int, proxy: Proxy
) -> Tuple[int, int, int]:
    params = {"id": str(rid)}
    ret = await fetch_proxy(session, ROOM_INIT_API, params, proxy)
    # print(ret)
    code = ret.get("code", -1)
    if code == 0:
        data = ret.get("data", None)
        if data:
            uid = data.get("uid", None)
            room_id = data.get("room_id", None)
            short_id = data.get("short_id", None)
            return room_id, short_id, uid
    else:
        raise RoomInitError(code, ret.get("msg"))


async def get_medal(session: aiohttp.ClientSession, rid: int, proxy: Proxy) -> Medal:
    room_id, short_id, uid = await get_room_info(session, rid, proxy)
    params = {"uid": str(uid)}
    ret = await fetch_proxy(session, LIVE_USER_API, params, proxy)
    # print(ret)
    code = ret.get("code", -1)
    if code == 0:
        data = ret.get("data", None)
        # print(rid)
        if data:
            medal_name = data.get("medal_name", None)
            # print(data.get("medal_name", ""))
            if medal_name:
                return Medal(
                    room_id=room_id,
                    short_id=short_id,
                    uid=uid,
                    medal_name=medal_name,
                )
    else:
        raise LiveUserError(code, ret.get("msg"))


async def save_result(conn: Connection, result: Medal):
    if isinstance(result, dict):
        print(
            f'sid: {result["short_id"]}, room_id: {result["room_id"]}, medal: {result["medal_name"]}'
        )
        await conn.execute(
            "INSERT OR IGNORE INTO medals VALUES (?, ?, ?, ?)",
            (
                result["room_id"],
                result["short_id"],
                result["uid"],
                result["medal_name"],
            ),
        )
        await conn.commit()


async def worker(
    sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    conn: Connection,
    proxies: asyncio.PriorityQueue[Proxy],
    jobs: asyncio.PriorityQueue[Job],
):
    while True:
        async with sem:
            proxy = await proxies.get()
            proxies.task_done()
            if proxy.usable():
                proxies.put_nowait(proxy)
                job = await jobs.get()
                try:
                    if job.exhausted:
                        print(f"Job failed. rid: {job.rid}")
                    else:
                        print(job.rid)
                        medal = await get_medal(session, job.rid, proxy)
                        print(medal)
                        await save_result(conn, medal)
                except TimeoutError:
                    proxy.punish_timeout()
                    await jobs.put(job)
                except ClientConnectionError:
                    proxy.punish_connectError()
                    await jobs.put(job)
                except ClientResponseError as e:
                    if e.status == 412:
                        print(f"{str(proxy)} limit exceed.")
                        proxy.discard()
                        await jobs.put(job)
                    else:
                        job.fail()
                        await jobs.put(job)
                except RoomInitError as e:
                    if e.code != 60004:  # room not exists
                        print(f"{job.rid}: {e}")
                except LiveUserError as e:
                    print(f"{job.rid}: {e}")

                finally:
                    jobs.task_done()


async def get_proxies(
    session: aiohttp.ClientSession,
    proxies: asyncio.PriorityQueue[Proxy],
    api: str,
    frequency: int,
):
    while True:
        await get_proxy(session, proxies, api)
        await asyncio.sleep(frequency)


async def get_proxy(
    session: aiohttp.ClientSession, proxies: asyncio.PriorityQueue[Proxy], api: str
):
    proxy_res = await fetch(session, api)
    code = proxy_res.get("code")
    if code == 0:
        data = proxy_res.get("data")
        for proxy_data in data:
            proxy = Proxy(proxy_data["host"], proxy_data["port"])
            await proxies.put(proxy)


async def distributer(
    from_rid: int,
    to_rid: int,
    concurrency: int,
    jobs: asyncio.PriorityQueue[Job],
):
    rid = from_rid
    try:
        while rid <= to_rid:
            while not jobs.full():
                if rid <= to_rid:
                    job = Job(rid)
                    jobs.put_nowait(job)
                    rid += 1
                else:
                    break
            await jobs.join()
    except asyncio.CancelledError:
        print(f"End at {rid}")
    except Exception as e:
        print(str(e))
        print(f"Error occured at {rid}")


async def main(
    from_rid: int, to_rid: int, concurrency: int, frequency: int, proxy_api: str
):
    async with aiosqlite.connect("medal.db") as conn:
        await conn.execute(
            """CREATE TABLE IF NOT EXISTS medals (
            room_id INTEGER PRIMARY KEY,
            short_id INTEGER,
            uid INTEGER,
            medal_name TEXT
            )"""
        )
        await conn.commit()

        sem = asyncio.Semaphore(concurrency + 2)
        connector = aiohttp.TCPConnector(
            ssl=False, limit=concurrency + 1, use_dns_cache=True
        )

        proxies = asyncio.PriorityQueue(maxsize=concurrency * 2)
        jobs = asyncio.PriorityQueue(maxsize=concurrency)

        tasks = []

        rid = from_rid

        timeout = aiohttp.ClientTimeout(total=1)
        async with aiohttp.ClientSession(
            connector=connector,
            json_serialize=dumps,
            headers=HEADERS,
            raise_for_status=True,
            timeout=timeout,
        ) as session:

            try:
                tasks.append(
                    asyncio.create_task(
                        get_proxies(session, proxies, proxy_api, frequency)
                    )
                )

                distributer_task = asyncio.create_task(
                    distributer(from_rid, to_rid, concurrency, jobs)
                )

                tasks.append(distributer_task)

                for i in range(concurrency):
                    tasks.append(
                        asyncio.create_task(worker(sem, session, conn, proxies, jobs))
                    )

                done, pending = await asyncio.wait(tasks, return_when="FIRST_COMPLETED")
                try:
                    for task in done:
                        task.result()
                    await jobs.join()
                except Exception as e:
                    print(str(e))
                    print("Error occured")
                print("finished!")

            except KeyboardInterrupt:
                distributer_task.cancel()

            except Exception as e:
                print(str(e))
                print(f"teminated at {rid}")

            finally:
                for task in tasks:
                    task.cancel()


parser = argparse.ArgumentParser()
parser.add_argument("from_rid", type=int)
parser.add_argument("to_rid", type=int)
parser.add_argument("-c", action="store", type=int, default=200)
parser.add_argument("-f", action="store", type=int, default=1)

if __name__ == "__main__":
    args = parser.parse_args()
    proxy_api = toml.load("config.toml")["PROXY_API"]
    start_time = time.time()
    asyncio.run(main(args.from_rid, args.to_rid, args.c, args.f, proxy_api))
    print(" %.2f seconds" % (time.time() - start_time))
