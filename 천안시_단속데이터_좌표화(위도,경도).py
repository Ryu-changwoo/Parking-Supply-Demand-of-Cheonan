# -*- coding: utf-8 -*-
"""
천안시 불법주정차 단속장소 지오코딩 (카카오맵 API)
- C열(단속동) + D열(단속장소) 결합
- D열이 숫자만이면 '{단속동} {숫자}번지'로 보정
- 주소 API 실패 -> 키워드 API 폴백 -> 행정동 센트로이드 대체
- 캐시/실패내역/체크포인트 저장, 병렬+레이트리밋, 숫자좌표/국내범위 가드
- CSV 인코딩 자동 탐지(utf-8-sig, utf-8, cp949, euc-kr)
"""

import os
import re
import json
import time
import requests
import pandas as pd
from typing import Tuple, Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from threading import Lock

# =========================
# 설정: 파일 경로 (필요시 변경)
# =========================
INPUT_CSV      = "data/충청남도_천안시_불법주정차단속현황_2022_clean_text_removed.csv"
OUTPUT_CSV     = "data/천안시_단속장소_위도경도_22년.csv"
FAILED_CSV     = "data/geocode_failed_22년.csv"
CHECKPOINT_CSV = "data/천안시_단속장소_위도경도_22년_checkpoint.csv"

# 캐시 (주소쿼리 ↔ 좌표), (행정동 ↔ 센트로이드)
CACHE_ADDR_JSON = "data/kakao_geocode_cache.json"
CACHE_DONG_JSON = "data/kakao_geocode_dong_centroid.json"

# 지오코딩 기본 설정
CITY_PREFIX       = "충청남도 천안시"  # ★ 보다 정확한 검색을 위해 도/시 포함
MAX_WORKERS       = 6                 # 동시 스레드 수
RATE_LIMIT_DELAY  = 0.30              # 전역 rate-limit 간격(초)
REQUEST_TIMEOUT   = 8                 # 초
RETRY_TOTAL       = 5
BACKOFF_FACTOR    = 0.7

# =========================
# Kakao 키 로딩 (환경변수/로컬파일)
# =========================
KAKAO_KEY = os.getenv("KAKAO_KEY", "").strip() or "43543891273a30b9398f2028f3ec4e61"


def make_headers() -> Dict[str, str]:
    return {"Authorization": f"KakaoAK {KAKAO_KEY}"}

# =========================
# 유틸: 주소 전처리 & 판별
# =========================
# 자주 나오는 모호/불필요 토큰
BAD_TOKENS = r"(부근|주변|앞|맞은편|인근|부근의|근처|일대|주차장입구|주출입구|사거리|교차로|방면|방향|건너편|옆|맞은|부지)"

def clean_place(s: str) -> str:
    """단속장소 클린업: 불필요 토큰/괄호 제거, 공백 정리"""
    if not isinstance(s, str):
        return ""
    s = re.sub(BAD_TOKENS, " ", s)
    s = re.sub(r"\(.*?\)", " ", s)                 # 괄호 내용 제거
    s = re.sub(r"[^\w\s\-\/·\.\d가-힣]", " ", s)    # 한글/숫자/일부기호 허용
    s = re.sub(r"\s+", " ", s).strip()
    return s

_num_pattern = re.compile(r"^\d+(-\d+){0,2}$")  # 123 | 123-4 | 123-4-5

def is_numeric_only_place(s: str) -> bool:
    """장소가 '숫자' 또는 '숫자-숫자(-숫자)' 형태인지"""
    if not s:
        return False
    s = s.strip()
    return bool(_num_pattern.fullmatch(s))

def place_contains_dong(place: str, dong: str) -> bool:
    """장소 문자열에 단속동명이 이미 포함되어 있는지 (단순 포함)"""
    if not place or not dong:
        return False
    return dong in place

def build_query(dong: str, raw_place: str) -> str:
    """
    지오코딩용 쿼리 생성:
    - 숫자만이면 '{단속동} {숫자}번지'
    - 장소에 동명이 이미 있으면 '충청남도 천안시 {장소}'
    - 그 외 '충청남도 천안시 {단속동} {장소}'
    """
    dong = (dong or "").strip()
    place = clean_place(raw_place)

    if is_numeric_only_place(place):
        if dong:
            return f"{CITY_PREFIX} {dong} {place}번지"
        return f"{CITY_PREFIX} {place}"  # 동이 없으면 번지 보강 어려움

    if dong and not place_contains_dong(place, dong):
        return f"{CITY_PREFIX} {dong} {place}".strip()
    return f"{CITY_PREFIX} {place}".strip()

# =========================
# 세션/재시도 설정
# =========================
def make_session() -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def is_number_like(v) -> bool:
    try:
        v = str(v).strip()
        if v == "":
            return False
        float(v)
        return True
    except Exception:
        return False

# 국내 위도/경도 대략 가드(대한민국 본토 범위 근사)
def in_korea_bounds(lat: float, lng: float) -> bool:
    return (33.0 <= lat <= 43.5) and (124.0 <= lng <= 132.5)

# =========================
# Kakao Geocoding
# =========================
def geocode_address(session: requests.Session, q: str) -> Tuple[Optional[float], Optional[float]]:
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    try:
        r = session.get(url, headers=make_headers(), params={"query": q}, timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        return None, None
    if r.status_code != 200:
        return None, None
    docs = r.json().get("documents", [])
    for d in docs:
        ra = d.get("road_address")
        if ra and is_number_like(ra.get("y")) and is_number_like(ra.get("x")):
            lat, lng = float(ra["y"]), float(ra["x"])
            return (lat, lng) if in_korea_bounds(lat, lng) else (None, None)
        ad = d.get("address")
        if ad and is_number_like(ad.get("y")) and is_number_like(ad.get("x")):
            lat, lng = float(ad["y"]), float(ad["x"])
            return (lat, lng) if in_korea_bounds(lat, lng) else (None, None)
    return None, None

def geocode_keyword(session: requests.Session, q: str) -> Tuple[Optional[float], Optional[float]]:
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    try:
        r = session.get(url, headers=make_headers(), params={"query": q}, timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        return None, None
    if r.status_code != 200:
        return None, None
    docs = r.json().get("documents", [])
    for d in docs:
        y, x = d.get("y"), d.get("x")
        if is_number_like(y) and is_number_like(x):
            lat, lng = float(y), float(x)
            return (lat, lng) if in_korea_bounds(lat, lng) else (None, None)
    return None, None

def geocode_any(session: requests.Session, q: str) -> Tuple[Optional[float], Optional[float]]:
    lat, lng = geocode_address(session, q)
    if lat is not None and lng is not None:
        return lat, lng
    return geocode_keyword(session, q)

# =========================
# 캐시 로드/세이브
# =========================
def load_cache(path: str) -> Dict[str, List[Optional[float]]]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(path: str, cache: Dict[str, List[Optional[float]]]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# =========================
# 행정동 센트로이드 (지오코딩 기반 대체)
# =========================
def get_dong_centroid(session: requests.Session, dong: str, cache_dong: Dict[str, List[Optional[float]]]) -> Tuple[Optional[float], Optional[float]]:
    """행정동 중심좌표: 행정복지센터 키워드 → 실패 시 동명 주소로 추정"""
    if not dong:
        return None, None
    if dong in cache_dong and len(cache_dong[dong]) == 2:
        lat, lng = cache_dong[dong]
        return (lat, lng) if (lat is not None and lng is not None) else (None, None)

    # 1) 행정복지센터(POI) 우선
    q1 = f"{CITY_PREFIX} {dong} 행정복지센터"
    lat, lng = geocode_keyword(session, q1)
    if lat is None or lng is None:
        # 2) 동명 자체를 주소로
        q2 = f"{CITY_PREFIX} {dong}"
        lat, lng = geocode_address(session, q2)
        if lat is None or lng is None:
            lat, lng = geocode_keyword(session, q2)

    cache_dong[dong] = [lat, lng] if (lat is not None and lng is not None) else [None, None]
    return lat, lng

# =========================
# CSV 인코딩 자동 로더
# =========================
def read_csv_auto(path: str, encodings=("utf-8-sig", "utf-8", "cp949", "euc-kr")) -> pd.DataFrame:
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
        except Exception as e:
            last_err = e
            continue
    raise last_err if last_err else RuntimeError("CSV 읽기 실패")

# =========================
# 메인 파이프라인
# =========================
def main():
    # 1) CSV 로드 (인코딩 자동)
    df = read_csv_auto(INPUT_CSV)

    # 필수 컬럼 확인/보강
    if "단속장소" not in df.columns:
        # D열이 '단속장소'가 아닌 경우, 4번째 컬럼을 단속장소로 간주
        try:
            df.rename(columns={df.columns[3]: "단속장소"}, inplace=True)
        except Exception:
            raise ValueError("CSV에 '단속장소' 컬럼이 없습니다.")
    if "단속동" not in df.columns:
        # C열을 단속동으로 간주
        try:
            df.rename(columns={df.columns[2]: "단속동"}, inplace=True)
        except Exception:
            df["단속동"] = ""

    # 2) 쿼리 구성
    df["단속동"] = df["단속동"].fillna("").astype(str).str.strip()
    df["단속장소"] = df["단속장소"].fillna("").astype(str).str.strip()
    df["쿼리주소"] = df.apply(lambda r: build_query(r["단속동"], r["단속장소"]), axis=1)

    # 3) 캐시 로드
    cache_addr = load_cache(CACHE_ADDR_JSON)   # 주소쿼리 캐시
    cache_dong = load_cache(CACHE_DONG_JSON)   # 동 센트로이드 캐시

    # 4) 세션/레이트리밋
    session = make_session()
    rate_lock = Lock()
    last_call_time = [0.0]

    def rate_limit_sleep():
        with rate_lock:
            now = time.time()
            elapsed = now - last_call_time[0]
            if elapsed < RATE_LIMIT_DELAY:
                time.sleep(RATE_LIMIT_DELAY - elapsed)
            last_call_time[0] = time.time()

    # 5) 고유 쿼리 우선순위 (빈도 높은 것부터)
    priority = df["쿼리주소"].value_counts().rename_axis("addr").reset_index(name="cnt")
    unique_addrs = priority["addr"].tolist()

    results: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    # 캐시에 있는 것은 미리 반영
    for a in unique_addrs:
        if a in cache_addr and isinstance(cache_addr[a], list) and len(cache_addr[a]) == 2:
            results[a] = (cache_addr[a][0], cache_addr[a][1])

    addrs_to_fetch = [a for a in unique_addrs if a not in results]
    print(f"총 고유 쿼리: {len(unique_addrs)} / 새 조회: {len(addrs_to_fetch)}")

    # 6) 병렬 지오코딩 함수
    def fetch(addr: str):
        rate_limit_sleep()
        lat, lng = geocode_any(session, addr)
        # 실패 시, addr에서 동명을 추출해서 센트로이드 대체
        if lat is None or lng is None:
            tail = addr.replace(CITY_PREFIX, "", 1).strip()
            dong = tail.split()[0] if tail else ""
            if dong:
                rate_limit_sleep()
                lat, lng = get_dong_centroid(session, dong, cache_dong)
        return addr, lat, lng

    # 7) 병렬 실행 + 체크포인트
    failed_addrs = []
    processed = 0
    checkpoint_every = 300

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(CHECKPOINT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(FAILED_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(CACHE_ADDR_JSON), exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch, a) for a in addrs_to_fetch]
        for fut in as_completed(futures):
            addr, lat, lng = fut.result()
            results[addr] = (lat, lng)
            cache_addr[addr] = [lat, lng] if (lat is not None and lng is not None) else [None, None]
            if lat is None or lng is None:
                failed_addrs.append(addr)
            processed += 1

            if processed % checkpoint_every == 0:
                # 캐시 저장
                save_cache(CACHE_ADDR_JSON, cache_addr)
                save_cache(CACHE_DONG_JSON, cache_dong)
                # 체크포인트 CSV
                df["위도"] = df["쿼리주소"].map(lambda a: results.get(a, (None, None))[0])
                df["경도"] = df["쿼리주소"].map(lambda a: results.get(a, (None, None))[1])
                df["지오코딩성공"] = df["위도"].notna() & df["경도"].notna()
                df.to_csv(CHECKPOINT_CSV, index=False, encoding="utf-8-sig")
                print(f"[Checkpoint] {processed}건 처리, 임시 저장 완료")

    # 8) 최종 매핑/저장
    df["위도"] = df["쿼리주소"].map(lambda a: results.get(a, (None, None))[0])
    df["경도"] = df["쿼리주소"].map(lambda a: results.get(a, (None, None))[1])
    df["지오코딩성공"] = df["위도"].notna() & df["경도"].notna()

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"[완료] 저장: {OUTPUT_CSV} | 성공률: {df['지오코딩성공'].mean()*100:.1f}% ({df['지오코딩성공'].sum()} / {len(df)})")

    # 9) 실패 목록 저장
    if failed_addrs:
        pd.DataFrame({"failed_query": sorted(set(failed_addrs))}).to_csv(FAILED_CSV, index=False, encoding="utf-8-sig")
        print(f"[참고] 실패 쿼리 {len(set(failed_addrs))}건 저장: {FAILED_CSV}")

    # 10) 캐시 저장
    save_cache(CACHE_ADDR_JSON, cache_addr)
    save_cache(CACHE_DONG_JSON, cache_dong)

if __name__ == "__main__":
    main()
