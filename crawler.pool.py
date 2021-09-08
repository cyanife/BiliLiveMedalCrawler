import argparse
import asyncio
import math
import time
from asyncio.locks import Semaphore
from typing import Tuple, TypedDict

import aiohttp
import aiosqlite
import uvloop
from aiohttp.client_exceptions import ClientConnectionError, ClientResponseError
from aiosqlite.core import Connection
from async_timeout import timeout
from rapidjson import dumps, loads

uvloop.install()


LIVE_USER_API = "http://api.live.bilibili.com/live_user/v1/Master/info"
# param: uid=x
ROOM_INIT_API = "http://api.live.bilibili.com/room/v1/Room/room_init"

PROXY_API = ""

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Referer": "https://live.bilibili.com/",
}


class RoomInitError(Exception):
    def __init__(self, code: int, msg: str) -> None:
        self._code = code
        self._msg = msg
        super().__init__(msg)

    def __str__(self) -> str:
        return f"[RoomInit] code: {self._code}, message: {self._msg}"


class LiveUserError(Exception):
    def __init__(self, code: int, msg: str) -> None:
        self._code = code
        self._msg = msg
        super().__init__(msg)

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

    def drop(self):
        self._score -= 3

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


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, str],
    proxy: Proxy,
) -> dict:
    async with session.get(url, params=params, proxy=proxy.url) as resp:
        return await resp.json(loads=loads)


async def get_room_info(
    session: aiohttp.ClientSession, rid: int, proxy: Proxy
) -> Tuple[int, int, int]:
    params = {"id": str(rid)}
    ret = await fetch(session, ROOM_INIT_API, params, proxy)
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
    ret = await fetch(session, LIVE_USER_API, params, proxy)
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
        print(f'room_id: {result["room_id"]}, medal: {result["medal_name"]}')
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


async def consumer(
    sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    conn: Connection,
    proxys: asyncio.PriorityQueue[Proxy],
    jobs: asyncio.Queue[Job],
):
    while True:
        async with sem:
            proxy = await proxys.get()
            job = await jobs.get()
            try:
                if job.exhausted:
                    print(f"Job failed. rid: {job.rid}")
                    await proxys.put(proxy)
                else:
                    medal = await get_medal(session, job.rid, proxy)
                    await save_result(conn, medal)
            except TimeoutError:
                proxy.punish_timeout()
                await proxys.put(proxy)
                await jobs.put(job)
            except ClientConnectionError:
                proxy.punish_connectError()
                await proxys.put(proxy)
                await jobs.put(job)
            except ClientResponseError as e:
                if e.status == 412:
                    print(f"{str(proxy)} limit exceed.")
                    await jobs.put(job)
                else:
                    job.fail()
                    await jobs.put(job)
                    await proxys.put(proxy)
            finally:
                proxys.task_done()
                jobs.task_done()


async def get_proxy(
    session: aiohttp.ClientSession,
    proxys: asyncio.PriorityQueue[Proxy],
):
    while True:
        proxy_res = await fetch(session, PROXY_API, None, None)
        code = proxy_res.get("code")
        if code == 0:
            data = proxy_res.get("data")
            for proxy_data in data:
                proxy = Proxy(proxy_data["host"], proxy_data["port"])
                await proxys.put(proxy)
        await asyncio.sleep(1)


async def main(from_rid: int, to_rid: int, concurrency: int):
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

    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(
        verify_ssl=False, limit=concurrency + 1, use_dns_cache=True
    )

    timeout = aiohttp.ClientTimeout(total=1)
    async with aiohttp.ClientSession(connector=connector,
        json_serialize=dumps, headers=HEADERS, raise_for_status=True, timeout=timeout
    ) as session:
        pass
