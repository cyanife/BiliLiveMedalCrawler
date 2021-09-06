import argparse
import re
import sqlite3
import time
from sqlite3.dbapi2 import Connection, Error
from typing import Tuple, TypedDict

import requests_html
from requests.exceptions import HTTPError

# LIVE_USER_API = "http://api.live.bilibili.com/live_user/v1/Master/info"
# param: uid=x

LIVE_ROOM_URL_PREFIX = "https://live.bilibili.com/"


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Referer": "https://live.bilibili.com/",
}

ROOM_INFO_PATTERN = re.compile(
    r"(?<=\"room_info\":{)\"uid\":(?P<uid>\d+),\"room_id\":(?P<room_id>\d+),\"short_id\":(?P<short_id>\d+)"
)

MEDAL_NAME_PATTERN = re.compile(r'(?<="medal_name":")[^"]*')


class Medal(TypedDict):
    room_id: int
    short_id: int
    uid: int
    medal_name: str


# def fetch_json(
#     session: requests_html.HTMLSession,
#     url: str,
#     params: dict[str, str],
# ) -> dict:
#     r = session.get(url, headers=HEADERS, params=params)
#     r.raise_for_status()
#     return r.json()


def fetch_html(
    session: requests_html.HTMLSession,
    url: str,
) -> str:
    r = session.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.html.html


def get_medal(session: requests_html.HTMLResponse, rid: int):
    try:
        url = LIVE_ROOM_URL_PREFIX + str(rid)
        html = fetch_html(session, url)
        medal_name = MEDAL_NAME_PATTERN.search(html)
        if medal_name:
            room_info = ROOM_INFO_PATTERN.search(html)
            if room_info:
                return Medal(
                    room_id=room_info["room_id"],
                    short_id=room_info["short_id"],
                    uid=room_info["uid"],
                    medal_name=medal_name[0],
                )
            else:
                return Medal(
                    room_id=rid, short_id=None, uid=None, medal_name=medal_name[0]
                )
        return None
    except HTTPError as e:
        if e.response.status_code == 404:
            return None
        else:
            raise e

    # with open("testnone.html", mode="x") as f:
    #     f.write(html.html)
    # medal_names = MEDAL_NAME_PATTERN.search(html)
    # print(medal_names.group())
    # room_info = ROOM_INFO_PATTERN.search(html)
    # print(room_info["uid"])

    # print(html.html)
    # space_link = html.xpath(
    #     "/html/body/div[1]/main/div[1]/section[1]/div[2]/div[1]/div/a"
    # )[0].href
    # uid = UID_PATTERN.match(space_link).group(0)
    # print(uid)


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


def main(from_rid: int, to_rid: int):
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

    with requests_html.HTMLSession() as s:
        for rid in range(from_rid, to_rid + 1):
            try:
                result = get_medal(s, rid)
                save_result(con, result)
            except HTTPError as e:
                if e.response.status_code == 412:
                    print("limit exceeded")
                    print(f"end at {rid}")
                    break
                else:
                    print(str(e))
                    continue
            except:
                print(f"end at {rid}")
                print("total: %s" % (rid - from_rid))
                raise


parser = argparse.ArgumentParser()
parser.add_argument("from_rid", type=int)
parser.add_argument("to_rid", type=int)

if __name__ == "__main__":
    args = parser.parse_args()
    start_time = time.time()
    main(args.from_rid, args.to_rid)
    print(" %.2f seconds" % (time.time() - start_time))
