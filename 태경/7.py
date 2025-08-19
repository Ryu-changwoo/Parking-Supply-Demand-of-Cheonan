'''
개요
입력: (1) 주차장 목록(df_park: 위도/경도/이름/ID/면수…)
      (2) 인구·세대·차량 데이터(동 단위 등)
크롤링: 주차장 별 반경 R 내 관심 카테고리(백화점·도서관·카페 등) 개수/최단거리 수집
집계: 표준화 후 가중합 → Infrastructure Score
결합: 인구·세대·차량 밀도 지표와 결합 → Demand Score
보정: (선택) 교통량/혼잡지표로 가산 → Priority Score
결과: CSV/지도(컬러마커·히트맵), 동별 탑N
'''

'''
1) 준비: 카카오 카테고리 검색으로 반경 내 POI 수집
카테고리 검색은 category_group_code + (x,y,radius) 혹은 rect 로 호출합니다. 
(반경은 0~20,000m, 응답은 최대 45건이라 영역 분할이 필요)
'''
# =========== 설정 ===========
import os, time, math, re, requests
import pandas as pd
from typing import List, Dict, Tuple

REST_API_KEY = os.getenv("KAKAO_REST_API_KEY") or "여기에_진짜키"
HEADERS = {"Authorization": f"KakaoAK {REST_API_KEY.strip()}"}

# 카카오 카테고리 코드 (주요만)
KAKAO_CATEGORIES = {
    "MT1":"대형마트","CS2":"편의점","PS3":"어린이집/유치원","SC4":"학교","AC5":"학원",
    "PK6":"주차장","OL7":"주유소","SW8":"지하철역","BK9":"은행","CT1":"문화시설",
    "AG2":"중개업소","PO3":"공공기관","AT4":"관광명소","AD5":"숙박","FD6":"음식점",
    "CE7":"카페","HP8":"병원","PM9":"약국"
}
# 우리 프로젝트에서 쓸 관심 카테고리(예시): 상업·문화·행정 유발요인
FOCUS_CODES = ["MT1","CT1","CE7","FD6","HP8","PM9","PO3","AT4","BK9","SC4"]
RADIUS_M = 300  # 반경 300m 기본값

def _call_category(code, x, y, radius=None, rect=None, page=1, size=15, timeout=10):
    url = "https://dapi.kakao.com/v2/local/search/category.json"
    params = {"category_group_code": code, "page": page, "size": size}
    if rect: params["rect"] = rect
    else:
        params.update({"x": x, "y": y})
        if radius: params["radius"] = radius
    r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
    if r.status_code == 429:
        time.sleep(0.7)
        r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
    if r.status_code != 200:
        print("[HTTP]", r.status_code, r.text[:300])
        r.raise_for_status()
    return r.json()

def _fetch_category_split_if_needed(code, x, y, radius_m, max_depth=4):
    """
    반경검색은 최대 45건까지만 내려오므로, 초과 시 원을 포함하는
    bbox(rect) 4등분 재귀로 더 수집하고 원거리 필터링(반경 내만)합니다.
    """
    # 1) 반경 한 번 호출
    j = _call_category(code, x, y, radius=radius_m, page=1)
    total = int(j["meta"].get("total_count", 0))
    docs = j.get("documents", [])
    # 페이지네이션 (그래도 45 상한)
    page = 1
    while not j["meta"].get("is_end", True) and page < 45:
        page += 1
        j = _call_category(code, x, y, radius=radius_m, page=page)
        docs += j.get("documents", [])

    if total <= 45 or max_depth <= 0:
        # 반경 안쪽만 남기기 (거리 계산)
        def _in_circle(d):
            dx = float(d["x"]) - x
            dy = float(d["y"]) - y
            # 경/위도 → 근사(m): 위도 1도≈111km, 경도는 cos(lat)
            m_per_deg_lat = 111_000
            m_per_deg_lon = math.cos(math.radians(y)) * 111_000
            dist = math.sqrt((dx*m_per_deg_lon)**2 + (dy*m_per_deg_lat)**2)
            return dist <= radius_m + 1e-6
        return [d for d in docs if _in_circle(d)]

    # 2) 초과: 원을 포함하는 bbox를 4등분(rect)으로 나눠 재귀
    # bbox 계산
    m_per_deg_lat = 111_000
    m_per_deg_lon = math.cos(math.radians(y)) * 111_000
    dlat = radius_m / m_per_deg_lat
    dlon = radius_m / m_per_deg_lon
    lx, rx, by, ty = x - dlon, x + dlon, y - dlat, y + dlat

    xm, ym = (lx+rx)/2, (by+ty)/2
    rects = [
        (lx, by, xm, ym),
        (xm, by, rx, ym),
        (lx, ym, xm, ty),
        (xm, ym, rx, ty),
    ]
    out = []
    for (x1, y1, x2, y2) in rects:
        rect_str = f"{x1},{y1},{x2},{y2}"
        # rect로도 45 초과면 더 쪼개기 위해 재귀
        jj = _call_category(code, x, y, rect=rect_str, page=1)  # x,y는 distance 계산용
        tt = int(jj["meta"].get("total_count", 0))
        if tt > 45 and max_depth > 0:
            out += _fetch_category_split_if_needed(code, x, y, radius_m, max_depth=max_depth-1)
        else:
            docs_r = jj.get("documents", [])
            page_r = 1
            while not jj["meta"].get("is_end", True) and page_r < 45:
                page_r += 1
                jj = _call_category(code, x, y, rect=rect_str, page=page_r)
                docs_r += jj.get("documents", [])
            out += docs_r

    # 원 안만 남기고 id 중복 제거
    seen, kept = set(), []
    for d in out:
        if d["id"] in seen: continue
        seen.add(d["id"]); kept.append(d)
    return _fetch_category_split_if_needed(code, x, y, radius_m, max_depth=0) + kept  # 마지막 한 번 반경 필터



'''
2) 주차장별로 카테고리 카운트/최단거리 피처 만들기
'''
path = "kakao_공영주차장.csv"
df_park = pd.read_csv(path, encoding="utf-8-sig")
df_park

def build_poi_features_for_parks(df_park: pd.DataFrame,
                                 lat_col="위도", lon_col="경도",
                                 focus_codes: List[str]=FOCUS_CODES,
                                 radius_m: int=RADIUS_M) -> pd.DataFrame:
    """
    각 주차장(한 행)마다 관심 카테고리별:
    - cnt_{code}: 반경 내 개수
    - dmin_{code}: 가장 가까운 곳까지의 거리(m), 없으면 NaN
    """
    rows = []
    for idx, r in df_park.iterrows():
        y = float(r[lat_col]); x = float(r[lon_col])
        row = {"park_idx": idx}
        for code in focus_codes:
            docs = _fetch_category_split_if_needed(code, x, y, radius_m=radius_m)
            row[f"cnt_{code}"] = len(docs)
            # 최단거리
            dmin = None
            for d in docs:
                dx = float(d["x"]) - x
                dy = float(d["y"]) - y
                m_per_deg_lat = 111_000
                m_per_deg_lon = math.cos(math.radians(y)) * 111_000
                dist = math.sqrt((dx*m_per_deg_lon)**2 + (dy*m_per_deg_lat)**2)
                dmin = dist if dmin is None else min(dmin, dist)
            row[f"dmin_{code}"] = dmin
            time.sleep(0.05)
        rows.append(row)
    feat = pd.DataFrame(rows).set_index("park_idx")
    return df_park.join(feat, how="left")





'''
3) 스코어링(가중합) + 정규화 템플릿
Infrastructure Score: 카테고리별 개수/거리의 표준화 + 가중합
Demand Score: (인구/세대/차량) 표준화 + 가중합
(선택)Traffic Score: 교통 혼잡/교통량 지표(정규화) 가산
Priority Score = Demand + Traffic + Infra − Supply(면수 정규화)
'''
def zscore(s: pd.Series):
    return (s - s.mean()) / (s.std(ddof=1) + 1e-9)

def build_scores(df: pd.DataFrame,
                 infra_weights: Dict[str,float]=None,
                 demand_cols: Dict[str,float]=None,
                 supply_col: str="총면수",
                 traffic_col: str=None):
    # 기본 가중치(예시) — 중요도 판단에 따라 조정 가능
    infra_weights = infra_weights or {
        "cnt_MT1": 2.0,  # 대형마트
        "cnt_CT1": 1.5,  # 문화시설
        "cnt_CE7": 1.0,  # 카페
        "cnt_FD6": 1.0,  # 음식점
        "cnt_HP8": 1.5,  # 병원
        "cnt_PM9": 1.0,  # 약국
        "cnt_PO3": 1.5,  # 공공기관
        "cnt_AT4": 1.0,  # 관광
        "cnt_BK9": 1.0,  # 은행
        "cnt_SC4": 1.0,  # 학교
        # 거리는 가까울수록 수요↑ → 음수부호로 반영(거리 짧을수록 점수↑)
        "dmin_MT1": -0.5, "dmin_CT1": -0.5, "dmin_HP8": -0.5,
    }
    demand_cols = demand_cols or {  # 동 단위 등에서 병합된 지표
        "총인구": 1.0,
        "세대수": 0.8,
        "등록차량": 1.2
    }

    df_sc = df.copy()

    # Infra score
    infra_terms = []
    for k, w in infra_weights.items():
        if k in df_sc.columns:
            infra_terms.append(w * zscore(df_sc[k].fillna(0)))
    df_sc["infra_score"] = sum(infra_terms) if infra_terms else 0

    # Demand score
    dem_terms = []
    for k, w in demand_cols.items():
        if k in df_sc.columns:
            dem_terms.append(w * zscore(df_sc[k].fillna(0)))
    df_sc["demand_score"] = sum(dem_terms) if dem_terms else 0

    # Supply(면수) — 많을수록 부족도↓ → 음수
    if supply_col in df_sc.columns:
        df_sc["supply_score"] = -1.0 * zscore(df_sc[supply_col].astype(float))
    else:
        df_sc["supply_score"] = 0

    # Traffic(선택)
    if traffic_col and traffic_col in df_sc.columns:
        df_sc["traffic_score"] = zscore(df_sc[traffic_col].astype(float))
    else:
        df_sc["traffic_score"] = 0

    # 최종
    df_sc["priority_score"] = df_sc["demand_score"] + df_sc["infra_score"] + df_sc["traffic_score"] + df_sc["supply_score"]
    return df_sc.sort_values("priority_score", ascending=False)



'''
4) 시각화: 컬러 마커(부족↑ 빨강), 히트맵/탑N 바차트
'''
import folium
from folium.plugins import MarkerCluster, HeatMap

def keep_cheonan(df_park: pd.DataFrame, strict=False) -> pd.DataFrame:
    """
    주소 문자열 기준으로 '천안시'만 남기고 이웃 도시(예: 아산시)는 제외.
    strict=True면 '천안시 동남구|천안시 서북구'까지 정확 매칭.
    """
    addr_candidates = ["도로명주소", "지번주소", "road_address", "address_name", "address"]
    addr_cols = [c for c in addr_candidates if c in df_park.columns]
    if not addr_cols:
        raise ValueError(f"주소 컬럼을 찾지 못했습니다. 현재 컬럼: {list(df_park.columns)}")

    s = df_park[addr_cols[0]].fillna("")
    for c in addr_cols[1:]:
        s = s.str.cat(df_park[c].fillna(""), sep="|")

    inc_pat = r"(천안시\s*동남구|천안시\s*서북구)" if strict else r"천안시"
    exc_pat = r"(아산시)"  # 필요시 이웃 도시 추가

    inc = s.str.contains(inc_pat, na=False)
    exc = s.str.contains(exc_pat, na=False)

    out = df_park[inc & ~exc].copy().reset_index(drop=True)
    print(f"[천안시 필터] 원본 {len(df_park)} → 천안시만 {len(out)} (strict={strict})")
    return out

def map_priority(df, lat_col="위도", lon_col="경도", name_col="상호명"):
    center = [df[lat_col].mean(), df[lon_col].mean()]
    m = folium.Map(location=center, zoom_start=12)
    mc = MarkerCluster().add_to(m)

    # 색상: 상위 33% 빨강, 중간 주황, 하위 파랑
    q66 = df["priority_score"].quantile(2/3)
    q33 = df["priority_score"].quantile(1/3)

    for _, r in df.iterrows():
        val = r["priority_score"]
        color = "red" if val >= q66 else ("orange" if val >= q33 else "blue")
        folium.Marker(
            [r[lat_col], r[lon_col]],
            tooltip=f'{r.get(name_col,"")}: {val:.2f}',
            popup=f"우선순위 {val:.2f}"
        ,icon=folium.Icon(color=color)).add_to(mc)

    # 히트맵(선택)
    HeatMap(df[[lat_col,lon_col,"priority_score"]].values.tolist(),
            radius=18, blur=14, max_zoom=14).add_to(m)
    return m



'''
5) 데이터 결합 팁 (면수/혼잡)

주차면수/유형/요금 등: 
전국주차장정보표준데이터(CSV/JSON)로 확보 가능 
(위도/경도, 구획수, 운영정보 포함) → 면수/유료 여부 등 공급 측 지표로 결합. 

천안시 공영주차장: 
천안도시공사/공공데이터포털에 주차면수·좌표가 정리된 최신 파일이 올라와 있습니다(2025-07-16 등록). 
바로 면수 결합용으로 좋습니다. 

교통량/혼잡(선택):
- 공공: **KTDB(국가교통DB)**에 시군·도로별 교통량 지표(시간대별/평균 등)와 각종 통계가 있습니다. 
        다운로드/조회로 혼잡 대용지표 생성 가능. 
- 상용/실시간: **TMAP 오픈API(교통 정보 검색)**로 주변 실시간 혼잡/돌발 조회 가능(별도 키 필요). 
              실시간을 점수화(혼잡 레벨 평균)해 반영 가능합니다. 
'''



'''
6) 실행 예 (끝까지 한번에)
아래 순서만 붙여 돌리면, 주차장별 인프라 점수 + 최종 우선순위 + 지도까지 나옵니다.
'''
import pandas as pd


# 1) 주차장 CSV 불러오기 (또는 메모리에 df가 있으면 그걸 사용)
try:
    df_park = pd.read_csv("kakao_공영주차장.csv")
except FileNotFoundError:
    if 'df' in globals():
        df_park = df.copy()
    else:
        raise RuntimeError("kakao_공영주차장.csv 파일이 없고, 메모리에 df도 없습니다.")

# 2) 컬럼 표준화 (있을 때만 rename)
rename_map = {
    'X':'경도', 'Y':'위도', 'stores':'상호명', 'place_url':'URL',
    'road_address':'도로명주소', 'address_name':'지번주소', 'id':'ID'
}
df_park = df_park.rename(columns={k:v for k,v in rename_map.items() if k in df_park.columns})

# 3) 필수 컬럼 확인/정리
for c in ['위도','경도']:
    if c not in df_park.columns:
        raise ValueError(f"필수 컬럼 누락: {c}. 현재 컬럼: {list(df_park.columns)}")
df_park['위도'] = pd.to_numeric(df_park['위도'], errors='coerce')
df_park['경도'] = pd.to_numeric(df_park['경도'], errors='coerce')
df_park = df_park.dropna(subset=['위도','경도']).reset_index(drop=True)
df_park = keep_cheonan(df_park, strict=False)

# 4) 피처 생성 → 스코어링 → 지도 (면수 없으면 supply_col=None)
has_supply = '총면수' in df_park.columns
df_feat = build_poi_features_for_parks(
    df_park, lat_col="위도", lon_col="경도",
    focus_codes=FOCUS_CODES, radius_m=300
)

# (선택) 인구/세대/차량 데이터 머지
# df_demo = pd.read_csv("cheonan_demo_by_dong.csv")  # 동, 총인구, 세대수, 등록차량 ...
# df_feat = df_feat.merge(df_demo, on="동", how="left")

df_scored = build_scores(
    df_feat,
    demand_cols={"총인구":1.0, "세대수":0.8, "등록차량":1.2},
    supply_col=("총면수" if has_supply else None),
    traffic_col=None
)

df_scored.head()
df_scored.columns

df_scored[["상호명","priority_score","infra_score","demand_score","supply_score"]].head(20)

m = map_priority(df_scored, lat_col="위도", lon_col="경도", name_col="상호명")
m.save("parking_priority_map.html")


map_path = "parking_priority_map.html"  # 또는 parking_priority_map_cheonan.html
import os, webbrowser
webbrowser.open('file://' + os.path.realpath(map_path))






##############################################################
print("len(df_park)  =", len(df_park))

df_feat = build_poi_features_for_parks(
    df_park, lat_col="위도", lon_col="경도",
    focus_codes=FOCUS_CODES, radius_m=300
)
print("len(df_feat)  =", len(df_feat))

df_scored = build_scores(
    df_feat,
    demand_cols={"총인구":1.0,"세대수":0.8,"등록차량":1.2},
    supply_col=("총면수" if "총면수" in df_park.columns else None),
    traffic_col=None
)
print("len(df_scored)=", len(df_scored))