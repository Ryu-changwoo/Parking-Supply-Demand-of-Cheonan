# kakao_local_crawl.py
import os
import re
import time
import math
import requests
import pandas as pd

# (선택) 지도 저장까지 하고 싶으면 Folium 설치 후 주석 해제
# pip install folium
try:
    import folium
    from folium.plugins import MiniMap
    HAS_FOLIUM = True
except Exception:
    HAS_FOLIUM = False


# =========================
# 0) 설정: 키 입력/검증
# =========================
# 방법 A) 환경변수로 넣기:  setx KAKAO_REST_API_KEY "xxxxxxxx..."
REST_API_KEY = (os.getenv("KAKAO_REST_API_KEY") or "").strip()

# 방법 B) 직접 문자열로 넣기 (ASCII만! 공백/개행X)
# REST_API_KEY = "0123456789abcdef0123456789abcdef"  # 예시형태(32자리 hex처럼 보임)

def _validate_key(key: str):
    if not key:
        raise ValueError("REST_API_KEY가 비었습니다. 환경변수나 코드에 실제 'REST API 키'를 넣어주세요.")
    if not key.isascii():
        raise ValueError("REST_API_KEY에 ASCII 이외 문자가 포함되어 있습니다. (한글/특수문자 금지)")
    # 형식은 앱마다 다를 수 있어 32자리 hex 고정 아님. 최소 길이/ASCII만 체크.
    if len(key) < 20:
        raise ValueError("REST_API_KEY 길이가 비정상적으로 짧습니다. Kakao 'REST API 키'가 맞는지 확인하세요.")

_validate_key(REST_API_KEY)
HEADERS = {"Authorization": f"KakaoAK {REST_API_KEY}"}


# =========================
# 1) 카카오 호출 래퍼
# =========================
def call_kakao_keyword(keyword: str, page: int = 1, size: int = 15, rect: str | None = None, timeout: int = 10):
    """
    Kakao Local Keyword Search 호출
    - keyword: 검색어 (예: '공영주차장')
    - rect: 'minX,minY,maxX,maxY' (X=경도, Y=위도 / '좌하단경도,좌하단위도,우상단경도,우상단위도')
    """
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    params = {"query": keyword, "page": page, "size": size}
    if rect:
        params["rect"] = rect

    resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)

    # 429(Too Many Requests) 방어: 짧게 대기 후 1회 재시도
    if resp.status_code == 429:
        time.sleep(0.6)
        resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)

    if resp.status_code != 200:
        # 디버깅 도움: 본문 앞부분 출력
        print(f"[HTTP {resp.status_code}] {resp.text[:300]}")
        resp.raise_for_status()

    return resp.json()


# =========================
# 2) 한 사각형(rect) 수집 (자동 4분할)
# =========================
def fetch_in_rect(keyword: str, x1: float, y1: float, x2: float, y2: float,
                  max_pages: int = 45, min_cell: float = 1e-6, depth: int = 0, max_depth: int = 20):
    """
    - 해당 rect에서 total_count가 45개 초과면 4분할(재귀),
      아니면 page 넘어가며 수집 (is_end True까지, 안전장치로 page<=max_pages)
    """
    # 좌표 정리(혹시 x1>x2, y1>y2 들어와도 처리)
    lx, rx = sorted([x1, x2])   # 경도
    by, ty = sorted([y1, y2])   # 위도
    rect_str = f"{lx},{by},{rx},{ty}"

    j = call_kakao_keyword(keyword, page=1, size=15, rect=rect_str)
    meta = j.get("meta", {})
    total = int(meta.get("total_count", 0))

    # 45 초과면 4분할 (무한분할 방지: 면적/깊이 제한)
    area_small = (abs(rx - lx) < min_cell) or (abs(ty - by) < min_cell)
    if total > 45 and (not area_small) and depth < max_depth:
        mx = (lx + rx) / 2.0
        my = (by + ty) / 2.0
        return (
            fetch_in_rect(keyword, lx, by, mx, my, max_pages, min_cell, depth + 1, max_depth) +
            fetch_in_rect(keyword, mx, by, rx, my, max_pages, min_cell, depth + 1, max_depth) +
            fetch_in_rect(keyword, lx, my, mx, ty, max_pages, min_cell, depth + 1, max_depth) +
            fetch_in_rect(keyword, mx, my, rx, ty, max_pages, min_cell, depth + 1, max_depth)
        )

    # 이하면 페이지네이션으로 수집
    out = []
    page = 1
    while True:
        jj = j if page == 1 else call_kakao_keyword(keyword, page=page, size=15, rect=rect_str)
        out.extend(jj.get("documents", []))

        m = jj.get("meta", {})
        if m.get("is_end", True):
            break

        page += 1
        if page > max_pages:  # 안전장치
            break
        time.sleep(0.05)     # 너무 빠르면 429 가능

    return out


# =========================
# 3) 큰 지역을 격자로 쪼개 스캔
# =========================
def scan_grid(keyword: str, x1: float, y1: float, x2: float, y2: float, nx: int, ny: int):
    """
    (x1,y1)~(x2,y2) 큰 사각형을 nx*ny 격자로 쪼개어 각각 fetch_in_rect 실행
    """
    lx, rx = sorted([x1, x2])
    by, ty = sorted([y1, y2])

    step_x = (rx - lx) / nx
    step_y = (ty - by) / ny

    all_docs = []
    for ix in range(nx):
        cx1 = lx + ix * step_x
        cx2 = cx1 + step_x
        for iy in range(ny):
            cy1 = by + iy * step_y
            cy2 = cy1 + step_y
            docs = fetch_in_rect(keyword, cx1, cy1, cx2, cy2)
            all_docs.extend(docs)
            time.sleep(0.03)
    return all_docs


# =========================
# 4) 후처리: DF 변환/중복제거/저장/지도
# =========================
def to_dataframe(docs: list[dict]) -> pd.DataFrame:
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    # id 기준 중복 제거
    if "id" in df.columns:
        df = df.drop_duplicates(subset="id")
    # 핵심 컬럼만 정리
    keep = ["id", "place_name", "category_name", "address_name", "road_address_name",
            "x", "y", "phone", "place_url"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()
    # 숫자형 변환
    if "x" in df.columns: df["x"] = df["x"].astype(float)
    if "y" in df.columns: df["y"] = df["y"].astype(float)
    # 보기 좋게 컬럼명 영→한 변경(원하시면 수정)
    rename = {
        "id": "ID",
        "place_name": "상호명",
        "category_name": "카테고리",
        "address_name": "지번주소",
        "road_address_name": "도로명주소",
        "x": "경도",
        "y": "위도",
        "phone": "전화",
        "place_url": "URL",
    }
    return df.rename(columns={k: v for k, v in rename.items() if k in df.columns})


def save_csv(df: pd.DataFrame, keyword: str, path: str | None = None) -> str:
    if df.empty:
        return ""
    safe_kw = re.sub(r"\s+", "_", keyword.strip())
    fname = path or f"kakao_{safe_kw}.csv"
    df.to_csv(fname, index=False, encoding="utf-8-sig")
    return fname


def save_map(df: pd.DataFrame, keyword: str, path: str | None = None) -> str:
    if df.empty or not HAS_FOLIUM:
        return ""
    lat = df["위도"].mean()
    lon = df["경도"].mean()
    m = folium.Map(location=[lat, lon], zoom_start=12)
    m.add_child(MiniMap())
    for _, r in df.iterrows():
        folium.Marker(
            [r["위도"], r["경도"]],
            tooltip=r.get("상호명", ""),
            popup=r.get("URL", "")
        ).add_to(m)
    safe_kw = re.sub(r"\s+", "_", keyword.strip())
    fname = path or f"kakao_{safe_kw}_map.html"
    m.save(fname)
    return fname


# =========================
# 5) 실행 예시
# =========================
if __name__ == "__main__":
    # 검색어
    keyword = "공영주차장"

    # ✅ 좌표 주의: rect는 (경도X, 위도Y) 순서입니다.
    # 예) 천안시 대략 bbox (대략치, 필요시 조정)
    #   좌하단(경도,위도): (127.05, 36.73)
    #   우상단(경도,위도): (127.22, 36.93)
    X1, Y1 = 127.05, 36.73
    X2, Y2 = 127.22, 36.93

    # 방법 1) 큰 영역을 격자로 스캔 (권장: 넓은 지역)
    docs = scan_grid(keyword, X1, Y1, X2, Y2, nx=8, ny=8)

    # 방법 2) 한 사각형만 스캔 (작은 영역이면 충분)
    # docs = fetch_in_rect(keyword, X1, Y1, X2, Y2)

    df = to_dataframe(docs)
    print(f"총 건수(중복제거 후): {len(df)}")

    if not df.empty:
        csv_path = save_csv(df, keyword)
        print(f"CSV 저장: {csv_path}")

        if HAS_FOLIUM:
            map_path = save_map(df, keyword)
            if map_path:
                print(f"지도 저장: {map_path}")
            else:
                print("Folium이 설치되지 않았거나 데이터가 비어 지도 저장을 건너뜀.")
    else:
        print("검색 결과가 없습니다. 좌표 범위/격자 크기/검색어를 조정해보세요.")
        

import os, webbrowser, folium
from folium.plugins import MiniMap, MarkerCluster

# ↓ df에 들어있는 위도/경도/이름 컬럼 자동 감지
lat_col = "위도" if "위도" in df.columns else ("Y" if "Y" in df.columns else None)
lon_col = "경도" if "경도" in df.columns else ("X" if "X" in df.columns else None)
name_col = "상호명" if "상호명" in df.columns else ("stores" if "stores" in df.columns else None)

assert lat_col and lon_col, "df에 위도/경도 컬럼(위도/경도 또는 X/Y)이 있어야 합니다."

m = folium.Map(location=[df[lat_col].astype(float).mean(),
                         df[lon_col].astype(float).mean()],
               zoom_start=12)
m.add_child(MiniMap())
mc = MarkerCluster().add_to(m)

for _, r in df.iterrows():
    folium.Marker(
        [float(r[lat_col]), float(r[lon_col])],
        tooltip=(str(r[name_col]) if name_col else ""),
        popup=str(r.get("URL", r.get("place_url","")))
    ).add_to(mc)

# 콘솔에서 바로 띄우기
map_path = "kakao_map_preview.html"
m.save(map_path)
webbrowser.open('file://' + os.path.realpath(map_path))


