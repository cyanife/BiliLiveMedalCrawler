import argparse
import asyncio
import math
import time
from asyncio.locks import Semaphore
from typing import Tuple, TypedDict

import aiohttp
import aiosqlite
import uvloop
from aiohttp.client_exceptions import ClientResponseError
from aiosqlite.core import Connection
from rapidjson import dumps, loads

uvloop.install()

LIVE_USER_API = "http://api.live.bilibili.com/live_user/v1/Master/info"
# param: uid=x
ROOM_INIT_API = "http://api.live.bilibili.com/room/v1/Room/room_init"
# param: id=xx
ROOM_INFO_API = "http://api.live.bilibili.com/room/v1/Room/get_info"
# param: room_id=xx


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Referer": "https://live.bilibili.com/",
}

chunk_size = 100


class Medal(TypedDict):
    room_id: int
    short_id: int
    uid: int
    medal_name: str


async def fetch(
    sem: Semaphore,
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, str],
    delay: float,
) -> dict:
    async with sem:
        await asyncio.sleep(delay)
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json(loads=loads)


async def get_room_info(
    sem: Semaphore, session: aiohttp.ClientSession, rid: int, delay: float
) -> Tuple[int, int, int]:
    params = {"id": str(rid)}
    ret = await fetch(sem, session, ROOM_INFO_API, params, delay)
    # print(ret)
    code = ret.get("code", -1)
    if code == 0:
        data = ret.get("data", None)
        if data:
            uid = data.get("uid", None)
            room_id = data.get("room_id", None)
            short_id = data.get("short_id", None)
            return room_id, short_id, uid
    return None, None, None


async def get_medal(
    sem: Semaphore, session: aiohttp.ClientSession, rid: int, delay: float
) -> Medal:
    room_id, short_id, uid = await get_room_info(sem, session, rid, delay)
    if uid:
        params = {"uid": str(uid)}
        ret = await fetch(sem, session, LIVE_USER_API, params, 0)
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
    return None


async def save_result(db: Connection, result: Medal):
    if isinstance(result, dict):
        print(f'room_id: {result["room_id"]}, medal: {result["medal_name"]}')
        await db.execute(
            "INSERT OR IGNORE INTO medals VALUES (?, ?, ?, ?)",
            (
                result["room_id"],
                result["short_id"],
                result["uid"],
                result["medal_name"],
            ),
        )
    # else:
    #     print(result)


async def main(from_rid: int, to_rid: int, frequency: int, delay: float):
    async with aiosqlite.connect("medal.db") as db:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS medals (
            room_id INTEGER PRIMARY KEY,
            short_id INTEGER,
            uid INTEGER,
            medal_name TEXT
            )"""
        )
        await db.commit()

        TTL = 0
        sem = asyncio.Semaphore(frequency)
        async with aiohttp.ClientSession(
            json_serialize=dumps, headers=HEADERS
        ) as session:
            total_num = to_rid - from_rid + 1
            chunk_num = math.ceil(total_num / chunk_size)
            chunk = 0
            while chunk < chunk_num:
                for rid in range(from_rid + chunk, to_rid + 1, chunk_num):
                    asyncio.create_task(get_medal(sem, session, rid, delay))
                    # print(rid)
                results = await asyncio.gather(
                    *asyncio.all_tasks() - {asyncio.current_task()},
                    return_exceptions=True,
                )
                for result in results:
                    if isinstance(result, ClientResponseError) and result.status == 412:
                        print("limit exceeded")
                        print("total: %s" % TTL)
                        return
                    else:
                        asyncio.create_task(save_result(db, result))
                    TTL += 1
                await asyncio.gather(
                    *asyncio.all_tasks() - {asyncio.current_task()},
                )
                await db.commit()
                chunk += 1
        print("total: %s" % TTL)


parser = argparse.ArgumentParser()
parser.add_argument("from_rid", type=int)
parser.add_argument("to_rid", type=int)
parser.add_argument("-f", action="store", type=int, default=10)
parser.add_argument("-d", action="store", type=float, default=0.1)

if __name__ == "__main__":
    args = parser.parse_args()
    start_time = time.time()
    asyncio.run(main(args.from_rid, args.to_rid, args.f, args.d))
    print(" %.2f seconds" % (time.time() - start_time))
