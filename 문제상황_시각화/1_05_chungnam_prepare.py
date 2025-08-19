import os
import pandas as pd
import numpy as np
from utils_common import (normalize_region, filter_chungnam, pick_sigungu_col,
                          coerce_numeric, percentile_rank)

# ===== 0) 경로 =====
POP_PATH = "data/충남_인구_세대.csv"
PARK_PATH = "data/전국_시군구_공영주차장.csv"
OUT_PATH = "output/chungnam_metrics.csv"
os.makedirs("output", exist_ok=True)

# ===== 1) 인구/세대 =====
pop = pd.read_csv(POP_PATH, encoding="cp949")
needed_cols = {"시군구명","남성인구수","여성인구수","세대수"}
missing = needed_cols - set(pop.columns)
if missing:
    raise ValueError(f"인구 데이터에 누락 컬럼: {missing}")

pop["남성인구수"] = coerce_numeric(pop["남성인구수"])
pop["여성인구수"] = coerce_numeric(pop["여성인구수"])
pop["세대수"]     = coerce_numeric(pop["세대수"])
pop["총인구"]     = pop["남성인구수"] + pop["여성인구수"]

# 매칭 키
pop["키"] = pop["시군구명"].apply(normalize_region)

# ===== 2) 주차장: 충남만 필터 + 시군구 집계 =====
park = pd.read_csv(PARK_PATH)  # 대부분 utf-8/기본으로 읽힘, 안되면 encoding="cp949"
park_chungnam = filter_chungnam(park)

sig_col = pick_sigungu_col(park_chungnam)
park_chungnam["키"] = park_chungnam[sig_col].apply(normalize_region)

park_count = (park_chungnam.groupby("키", as_index=False)
              .size().rename(columns={"size":"공영주차장_시설수"}))

# ===== 3) 병합 & 지표 =====
df = pop.merge(park_count, on="키", how="left")
df["공영주차장_시설수"] = df["공영주차장_시설수"].fillna(0).astype(int)

df["인구1만명당_주차시설"]   = df["공영주차장_시설수"] / (df["총인구"] / 10_000).replace(0, np.nan)
df["세대1000가구당_주차시설"] = df["공영주차장_시설수"] / (df["세대수"] / 1_000).replace(0, np.nan)

# ===== 4) 천안 요약 =====
cheonan_mask = df["시군구명"].astype(str).str.contains("천안")
cheonan = df[cheonan_mask].copy()

summary_rows = []
for _, r in cheonan.iterrows():
    pr_pop = percentile_rank(df["인구1만명당_주차시설"], r["인구1만명당_주차시설"])
    pr_hh  = percentile_rank(df["세대1000가구당_주차시설"], r["세대1000가구당_주차시설"])
    summary_rows.append({
        "시군구명": r["시군구명"],
        "총인구": int(r["총인구"]),
        "세대수": int(r["세대수"]),
        "공영주차장_시설수": int(r["공영주차장_시설수"]),
        "인구1만명당_주차시설": round(r["인구1만명당_주차시설"], 3),
        "세대1000가구당_주차시설": round(r["세대1000가구당_주차시설"], 3),
        "인구1만명당_백분위(여유)": round(pr_pop, 1),
        "세대1000가구당_백분위(여유)": round(pr_hh, 1),
    })
cheonan_summary = pd.DataFrame(summary_rows)

print("\n[천안 요약]")
print(cheonan_summary.to_string(index=False))

# 부족 지역 우선 표
cols_exist = [c for c in ["기준연도","시군구명","총인구","세대수","공영주차장_시설수",
                          "인구1만명당_주차시설","세대1000가구당_주차시설"]
              if c in df.columns]
result_sorted = df.sort_values(["인구1만명당_주차시설","세대1000가구당_주차시설"],
                               ascending=[True, True])[cols_exist]
print("\n[충남 시군구별 지표 (부족지역 우선 정렬, 상위 10)]")
print(result_sorted.head(10).to_string(index=False))

# ===== 5) 저장 (시각화 스크립트들이 이 파일만 읽으면 됨) =====
df_out = df.copy()
df_out["인구1만명당_주차시설_r"]   = df_out["인구1만명당_주차시설"].round(3)
df_out["세대1000가구당_주차시설_r"] = df_out["세대1000가구당_주차시설"].round(3)
df_out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
print(f"\n[저장 완료] {OUT_PATH}")
