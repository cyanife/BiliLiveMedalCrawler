import math
import argparse
import aiohttp
import asyncio
import uvloop
import rapidjson

uvloop.install()

LIVE_USER_API = "http://api.live.bilibili.com/live_user/v1/Master/info"  # param: uid=x
ROOM_INIT_API = "http://api.live.bilibili.com/room/v1/Room/room_init"  # param: id=xx

chunk_size = 10


async def fetch(session: aiohttp.ClientSession, url, params):
    async with session.get(url, params=params) as resp:
        print(resp)
        resp.raise_for_status()
        return await resp.json()


async def getUID(session, rid):
    params = {"id": str(rid)}
    ret = await fetch(session, ROOM_INIT_API, params=params)
    print(ret)
    data = ret.get("data", None)
    if data:
        # print(data.get("uid", 0))
        return data.get("uid", 0)


async def getMedal(session, rid):
    uid = await getUID(session, rid)
    params = {"uid": str(uid)}
    ret = await fetch(session, LIVE_USER_API, params=params)
    print(ret)
    data = ret.get("data", None)
    # print(rid)
    if data:
        # print(data.get("medal_name", ""))
        return data.get("medal_name", "")


async def main(from_rid: int, to_rid: int):
    async with aiohttp.ClientSession(json_serialize=rapidjson.dumps) as session:
        chunk_num = math.ceil((to_rid - from_rid) / chunk_size)
        chunk = 0
        while chunk < chunk_num:
            tasks = []
            for rid in range(from_rid + chunk, to_rid, chunk_num):
                tasks.append(asyncio.create_task(getMedal(session, rid)))
                print(rid)
            print("gather")
            res = await asyncio.wait(tasks, return_when="FIRST_EXCEPTION")
            print(res)
            chunk += 1
            print("chunk")
            print(chunk)


parser = argparse.ArgumentParser()
parser.add_argument("from_rid", type=int)
parser.add_argument("to_rid", type=int)

if __name__ == "__main__":
    args = parser.parse_args()
    asyncio.run(main(args.from_rid, args.to_rid))
