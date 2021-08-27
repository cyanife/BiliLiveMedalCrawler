import aiohttp
import asyncio
import uvloop
import rapidjson

uvloop.install()

LIVE_USER_API = "http://api.live.bilibili.com/live_user/v1/Master/info"  # param: uid=x
ROOM_INIT_API = "http://api.live.bilibili.com/room/v1/Room/room_init"  # param: id=xx

async def fetch(session, url, params):
    async with session.get(url, params=params) as resp:
        return await resp.json()


async def getUID(session, rid):
    params = {'id': str(rid)}
    ret = await fetch(session, ROOM_INIT_API, params=params)
    print(ret)
    data = ret.get("data", None)
    if data:
        print(data.get("uid", 0))
        return data.get("uid", 0)

async def getMedal(session, rid):
    uid = await getUID(session, rid)
    params = {'uid': str(uid)}
    ret = await fetch(session, LIVE_USER_API, params=params)
    data = ret.get("data", None)
    print(rid)
    if data:
        print(data.get('medal_name', ''))
        return data.get('medal_name', '')


async def main():
    async with aiohttp.ClientSession(json_serialize=rapidjson.dumps) as session:
        for rid in range(1,10):
            asyncio.create_task(getMedal(session, rid))
        await asyncio.gather(*asyncio.all_tasks() - {asyncio.current_task()})


asyncio.run(main())