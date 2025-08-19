from pathlib import Path
import json
import pandas as pd
import plotly.express as px
from pathlib import Path

# PC의 실제 폴더 경로 (raw string을 쓰면 \ 이스케이프 문제 없음)
BASE = Path(r"C:\Users\USER\Documents\DS Projects\lsbs-gen5\cheonan")

# 결과 저장 폴더(같은 위치에 out 생성)
OUTDIR = BASE / "out"
OUTDIR.mkdir(parents=True, exist_ok=True)

def read_csv_try(path, encodings=("utf-8-sig","utf-8","cp949","euc-kr","latin1")):
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            print(f"[OK] {path.name} ← encoding='{enc}'")
            return df
        except Exception as e:
            print(f"[FAIL] {path.name} encoding='{enc}': {e}")
            last_err = e
    raise last_err

def pick_col(cols, candidates):
    """후보 이름 중 실제 존재하는 컬럼을 찾아 반환"""
    lowmap = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lowmap:
            return lowmap[cand.lower()]
    return None

def normalize(s: pd.Series) -> pd.Series:
    """공백 제거(조인키 표준화)"""
    return s.astype(str).str.replace(r"\s+", "", regex=True)

# 주차장 CSV: 파일명에 '주차장' 또는 'parking'
# 인구·세대 CSV: 파일명에 '인구'와 '세대' 또는 'population'
# 시군구 SHP: 확장자 .shp 이고 이름에 'sig' 또는 '시군구' 또는 'sgg'
def auto_find_files(base: Path):
    parking_csv = None
    pop_csv = None
    sig_shp = None

    for p in base.rglob("*"):
        if not p.is_file():
            continue
        low = p.name.lower()
        # 주차장 CSV
        if p.suffix.lower()==".csv" and ("주차" in p.name or "parking" in low):
            if parking_csv is None:
                parking_csv = p
        # 인구·세대 CSV
        if p.suffix.lower()==".csv" and (("인구" in p.name and "세대" in p.name) or "population" in low):
            if pop_csv is None:
                pop_csv = p
        # 시군구 SHP
        if p.suffix.lower()==".shp" and ("sig" in low or "시군구" in p.name or "sgg" in low):
            if sig_shp is None:
                sig_shp = p

    if not parking_csv or not pop_csv or not sig_shp:
        raise FileNotFoundError(
            f"찾은 파일 → 주차장:{parking_csv}, 인구세대:{pop_csv}, SHP:{sig_shp}\n"
            "※ 파일명이 너무 일반적이면 못 찾을 수 있어요. 규칙(주차/parking, 인구+세대/population, sig/시군구/sgg)을 만족하게 파일명을 바꾸거나, 직접 경로를 지정해 주세요."
        )
    return parking_csv, pop_csv, sig_shp

# 실행: BASE는 0단계에서 만든 경로
parking_csv, pop_csv, sig_shp = auto_find_files(BASE)
print("주차장 CSV:", parking_csv)
print("인구·세대 CSV:", pop_csv)
print("시군구 SHP:", sig_shp)


pk = read_csv_try(parking_csv)

# 컬럼 자동 매핑
sido_col = pick_col(pk.columns, ["CTPRVN_NM","시도명","시도","광역시도"])
sig_col  = pick_col(pk.columns, ["SIGNGU_NM","시군구명","시군구","구군"])
name_col = pick_col(pk.columns, ["FCLTY_NM","시설명","주차장명"])
stall_col= pick_col(pk.columns, ["PARKNG_SPCE_CO","주차구획수","주차면수","총주차면","총주차대수"])

need = {"시도":sido_col, "시군구":sig_col}
miss = [k for k,v in need.items() if v is None]
if miss:
    raise KeyError(f"주차장 CSV에 필수 컬럼이 없습니다: {miss}\n현재 열: {list(pk.columns)}")

# '충남' 또는 '충청남' 포함 행만
pk_chn = pk[pk[sido_col].astype(str).str.contains("충남|충청남", regex=True, na=False)].copy()
if stall_col:
    pk_chn["_STALL"] = pd.to_numeric(pk_chn[stall_col], errors="coerce")

agg_dict = {"공영주차장_수": (name_col or sig_col, "count")}
if stall_col:
    agg_dict["주차구획수_합계"] = ("_STALL","sum")

agg = (pk_chn.groupby(sig_col, as_index=False)
              .agg(**agg_dict)
              .rename(columns={sig_col:"시군구"})
              .sort_values("시군구"))

agg_path = OUTDIR / "충남_공영주차장_집계.csv"
agg.to_csv(agg_path, index=False, encoding="utf-8-sig")
agg.head()


pop = read_csv_try(pop_csv)

sig2  = pick_col(pop.columns, ["시군구명","시군구","자치구","구군"])
year  = pick_col(pop.columns, ["기준연도","연도","year"])
male  = pick_col(pop.columns, ["남성인구수","남자","m"])
female= pick_col(pop.columns, ["여성인구수","여자","f"])
fm    = pick_col(pop.columns, ["외국인남성인구수","외국인남자"])
ff    = pick_col(pop.columns, ["외국인여성인구수","외국인여자"])
hh    = pick_col(pop.columns, ["세대수","가구수","household"])

need_pop = {"시군구":sig2, "연도":year, "남":male, "여":female, "외남":fm, "외여":ff, "세대":hh}
missp = [k for k,v in need_pop.items() if v is None]
if missp:
    raise KeyError(f"인구·세대 CSV에 필요한 컬럼을 찾지 못했습니다: {missp}\n현재 열: {list(pop.columns)}")

for c in [year, male, female, fm, ff, hh]:
    pop[c] = pd.to_numeric(pop[c], errors="coerce")

latest = int(pop[year].max())
pop_latest = pop[pop[year]==latest].copy()
print("최신 연도:", latest)

pop_latest["상주인구"] = pop_latest[male].fillna(0) + pop_latest[female].fillna(0) + pop_latest[fm].fillna(0) + pop_latest[ff].fillna(0)
pop_sel = pop_latest.rename(columns={sig2:"시군구", hh:"세대수"})[["시군구","상주인구","세대수"]]
pop_sel.head()


merged = pop_sel.merge(agg, on="시군구", how="left")

for c in ["공영주차장_수","주차구획수_합계","상주인구","세대수"]:
    if c in merged.columns:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

merged["주차구획수_천명당"]   = (merged.get("주차구획수_합계",0) / merged["상주인구"].replace({0: pd.NA}) * 1000).round(2)
merged["주차구획수_천세대당"] = (merged.get("주차구획수_합계",0) / merged["세대수"].replace({0: pd.NA}) * 1000).round(2)

# 조인키(공백 제거)
merged["JOIN_NM"] = normalize(merged["시군구"])

merged_path = OUTDIR / "충남_병합지표.csv"
merged.to_csv(merged_path, index=False, encoding="utf-8-sig")
merged.sort_values("주차구획수_천명당", ascending=False).head()


import json
import fiona
# pip install fiona pyproj
from pyproj import Transformer

def shp_to_geojson_chungnam(sig_shp: Path, out_geo: Path):
    feats = []
    with fiona.open(sig_shp) as src:
        # 좌표계 추정 후 4326 변환 (없으면 5179 가정)
        epsg_in = None
        try:
            if isinstance(src.crs, dict) and "init" in src.crs:
                epsg_in = int(str(src.crs["init"]).split(":")[-1])
            elif isinstance(src.crs, dict) and "EPSG" in src.crs:
                epsg_in = int(src.crs["EPSG"])
        except Exception:
            pass
        try:
            transformer = Transformer.from_crs(epsg_in or 5179, 4326, always_xy=True)
        except Exception:
            transformer = None

        def reproj_coords(coords):
            if transformer is None:
                return coords
            if isinstance(coords[0], (float, int)):
                x, y = coords
                lon, lat = transformer.transform(x, y)
                return [lon, lat]
            else:
                return [reproj_coords(c) for c in coords]

        for feat in src:
            props = dict(feat["properties"])
            geom  = feat["geometry"]
            sig_cd  = str(props.get("SIG_CD",""))
            sido_nm = str(props.get("SIDO_NM",""))
            # 충남만: SIG_CD '44***' 또는 SIDO_NM=충청남도
            if not (sig_cd.startswith("44") or ("충청남도" in sido_nm)):
                continue

            if geom and "coordinates" in geom:
                try:
                    new_coords = reproj_coords(geom["coordinates"])
                    geom = {"type": geom["type"], "coordinates": new_coords}
                except Exception:
                    pass

            props["JOIN_NM"] = str(props.get("SIG_KOR_NM","")).replace(" ","")
            feats.append({"type":"Feature","properties":props,"geometry":geom})

    gj = {"type":"FeatureCollection","features":feats}
    with open(out_geo, "w", encoding="utf-8") as f:
        json.dump(gj, f, ensure_ascii=False)
    return out_geo

geo_path = OUTDIR / "충남_sig.geojson"
_ = shp_to_geojson_chungnam(sig_shp, geo_path)
print("생성:", geo_path)


import json
import plotly.express as px

with open(geo_path, "r", encoding="utf-8") as f:
    gj = json.load(f)

fig = px.choropleth(
    merged,
    geojson=gj,
    locations="JOIN_NM",
    color="주차구획수_천명당",
    featureidkey="properties.JOIN_NM",
    hover_name="시군구",
    hover_data={
        "상주인구": True,
        "세대수": True,
        "공영주차장_수": True,
        "주차구획수_합계": True,
        "주차구획수_천명당": True,
        "주차구획수_천세대당": True,
        "JOIN_NM": False
    },
    title="충청남도 시군구별 공영주차장 공급 지표 (인구 1천명당)"
)
fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin=dict(r=0, t=60, l=0, b=0))

html_out = OUTDIR / "충남_코로플레스.html"
fig.write_html(str(html_out), include_plotlyjs="cdn")  # Windows 경로 호환 위해 str()
fig.show()
print("지도 저장:", html_out)