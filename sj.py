import pandas as pd
import requests
import re
import time
import os

# CSV 경로
file_path = "data/충청남도 천안시_불법주정차단속현황_2024_08.CSV"
# CSV 읽기
df = pd.read_csv(file_path, encoding="cp949")

# 카카오 REST API 키 (환경변수 권장)
KAKAO_KEY = "29ccf6e9ddda5c5a1ed9180f883e059a"

def clean_addr(s: str) -> str:
    """괄호 및 불필요한 공백 제거"""

    if not isinstance(s, str):
        return ""
    if "부근" in s:
        s = s.replace("부근", "").strip()
    if "주변" in s:
        s = s.replace("주변", "").strip()

    if "," in s:
        s = s.split(",")[1]
    s = re.sub(r"\(.*?\)", "", s)  # 괄호 제거
    s = re.sub(r"\s+", " ", s)     # 공백 정리
    return s.strip()

def geocode_kakao(addr: str):
    """카카오 API로 주소 → 위도, 경도 변환"""
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {KAKAO_KEY}"}
    params = {"query": addr}
    r = requests.get(url, headers=headers, params=params)

    if r.status_code != 200:
        return None, None

    docs = r.json().get("documents", [])
    if not docs:
        return None, None

    # 도로명 주소가 있는 경우
    if docs[0].get("road_address"):
        y = docs[0]["road_address"]["y"]
        x = docs[0]["road_address"]["x"]
    else:
        y = docs[0]["address"]["y"]
        x = docs[0]["address"]["x"]

    # 좌표 값이 비어 있거나 변환 불가한 경우
    if not y or not x:
        return None, None

    try:
        lat = float(y)
        lng = float(x)
    except ValueError:
        return None, None

    return lat, lng


# 주소 정리
df["주소_정리"] = df["단속장소"].apply(clean_addr)

# 위도·경도 추출
lat_list, lng_list = [], []
for addr in df["주소_정리"]:
    lat, lng = geocode_kakao(f"천안시 {addr}")  # '천안시'를 붙이면 인식률↑
    lat_list.append(lat)
    lng_list.append(lng)
    print(f"주소: {addr}, 위도: {lat}, 경도: {lng}")
    if lat is None or lng is None:
        print(f"주소 '{addr}'에 대한 위도/경도 정보를 찾을 수 없습니다.")
    else:
        print(f"주소 '{addr}'의 위도: {lat}, 경도: {lng}")
    time.sleep(0.1)  # API 호출 간격

df["위도"] = lat_list
df["경도"] = lng_list

# 결과 출력
print(df[["단속장소", "위도", "경도"]])

# 결과 저장
output_file = "data/천안시_단속장소_위도경도.csv"
df.to_csv(output_file, index=False, encoding="utf-8-sig")
# 결과 파일 경로
print(f"결과가 '{output_file}'에 저장되었습니다.")