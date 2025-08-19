import pandas as pd

# 1) CSV 파일 경로 설정
in_path  = "data/천안시_단속장소_위도경도_24년.csv"
out_path = "data/천안시_단속장소_위도경도_24년_grouped.csv"

# 2) 인코딩 자동 시도하여 로드
df = None
for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
    try:
        df = pd.read_csv(in_path, encoding=enc, low_memory=False)
        print(f"[INFO] 로드 성공: {enc}")
        break
    except Exception as e:
        last_err = e
if df is None:
    raise last_err

# 3) 위도/경도 컬럼 찾기 (후보 이름 중 첫 매칭 사용)
lat_candidates = ["위도", "lat", "Lat", "LAT", "Latitude", "latitude"]
lng_candidates = ["경도", "lng", "lon", "Lng", "Lon", "LON", "Longitude", "longitude"]

lat_col = next((c for c in lat_candidates if c in df.columns), None)
lng_col = next((c for c in lng_candidates if c in df.columns), None)
if not lat_col or not lng_col:
    raise RuntimeError(f"위도/경도 컬럼을 찾지 못했습니다. 현재 컬럼: {list(df.columns)}")

# 4) 숫자화 + 결측 제거
before = len(df)
df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
df[lng_col] = pd.to_numeric(df[lng_col], errors="coerce")
df = df.dropna(subset=[lat_col, lng_col]).copy()
print(f"[INFO] 위경도 결측 제거: {before} -> {len(df)}")

# (옵션) 근접 지점 병합을 원하면 주석 해제: 소수 6자리 반올림(약 0.11 m)
# df[lat_col] = df[lat_col].round(6)
# df[lng_col] = df[lng_col].round(6)

# 5) (위도, 경도)별 단속건수 집계
grouped = (
    df.groupby([lat_col, lng_col], as_index=False)
      .size()
      .rename(columns={"size": "단속건수"})
      .sort_values("단속건수", ascending=False)
)

# 6) 저장
grouped.to_csv(out_path, index=False, encoding="utf-8-sig")
print(f"[DONE] 저장: {out_path}, 고유 좌표 수={len(grouped)}")
print(grouped.head(5))
