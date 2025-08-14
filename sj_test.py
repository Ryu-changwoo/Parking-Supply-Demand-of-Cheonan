import pandas as pd
import requests
import re
import time
import os

KAKAO_KEY = "29ccf6e9ddda5c5a1ed9180f883e059a"


def geocode_kakao(addr: str):
    """카카오 API로 주소 → 위도, 경도 변환"""
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_KEY}"}
    params = {"query": addr}
    r = requests.get(url, headers=headers, params=params)
    print(f"{r.status_code} {r.reason}")
    if r.status_code != 200:
        return None, None
    docs = r.json().get("documents", [])
    if not docs:
        return None, None
    if docs[0].get("road_address"):
        lat = float(docs[0]["road_address"]["y"])
        lng = float(docs[0]["road_address"]["x"])
    else:
        lat = float(docs[0]["address"]["y"])
        lng = float(docs[0]["address"]["x"])
    return lat, lng

a = lat, lng = geocode_kakao("서울특별시 중구 세종대로 110")

print(a)