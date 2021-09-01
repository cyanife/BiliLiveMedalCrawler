import argparse
import sqlite3
import time
from sqlite3.dbapi2 import Connection
from typing import Tuple, TypedDict

import requests
from requests.exceptions import HTTPError

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


class Medal(TypedDict):
    room_id: int
    short_id: int
    uid: int
    medal_name: str


def fetch(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    delay: float,
) -> dict:
    time.sleep(delay)
    r = session.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()


def get_room_info(
    session: requests.Session, rid: int, delay: float
) -> Tuple[int, int, int]:
    params = {"id": str(rid)}
    ret = fetch(session, ROOM_INFO_API, params, delay)
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


def get_medal(session: requests.Session, rid: int, delay: float) -> Medal:
    room_id, short_id, uid = get_room_info(session, rid, delay)
    if uid:
        params = {"uid": str(uid)}
        ret = fetch(session, LIVE_USER_API, params, 0)
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


def save_result(db: Connection, result: Medal):
    if isinstance(result, dict):
        print(f'room_id: {result["room_id"]}, medal: {result["medal_name"]}')
        db.execute(
            "INSERT OR IGNORE INTO medals VALUES (?, ?, ?, ?)",
            (
                result["room_id"],
                result["short_id"],
                result["uid"],
                result["medal_name"],
            ),
        )
        db.commit()
    # else:
    #     print(result)


def main(from_rid: int, to_rid: int, delay: float):
    con = sqlite3.connect("medal.db")
    con.execute(
        """CREATE TABLE IF NOT EXISTS medals (
        room_id INTEGER PRIMARY KEY,
        short_id INTEGER,
        uid INTEGER,
        medal_name TEXT
        )"""
    )
    con.commit()

    with requests.Session() as s:
        try:
            for rid in range(from_rid, to_rid + 1):
                print(rid)
                result = get_medal(s, rid, delay)
                save_result(con, result)

        except HTTPError as e:
            if e.response.status_code == 412:
                print("limit exceeded")
            print("total: %s" % (rid - from_rid))

    con.close()


parser = argparse.ArgumentParser()
parser.add_argument("from_rid", type=int)
parser.add_argument("to_rid", type=int)
parser.add_argument("-d", action="store", type=float, default=0.1)

if __name__ == "__main__":
    args = parser.parse_args()
    start_time = time.time()
    main(args.from_rid, args.to_rid, args.d)
    print(" %.2f seconds" % (time.time() - start_time))
