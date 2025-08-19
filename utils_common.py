import re
import pandas as pd
import unicodedata
import numpy as np
from typing import Iterable

# ===== 기존 유틸 =====
def find_region_col_pop(df):
    for c in df.columns:
        if ("행정구역" in c) or ("시도명" in c) or ("시군구명" in c):
            return c
    raise KeyError("인구 테이블에서 지역 열을 찾지 못했습니다.")

def latest_total_month_col(df):
    month_cols = [c for c in df.columns if re.fullmatch(r"\d{4}\.\d{2}", str(c))]
    if not month_cols:
        mset = set()
        for c in df.columns:
            m = re.match(r"(\d{4}\.\d{2})", str(c))
            if m: mset.add(m.group(1))
        month_cols = sorted(mset)
    if not month_cols:
        raise KeyError("인구 테이블에서 'YYYY.MM' 형태의 열을 찾지 못했습니다.")
    return sorted(month_cols)[-1]

def to_short_sido(name: str) -> str:
    s = str(name).strip()
    mapping = {
        "서울특별시":"서울","부산광역시":"부산","인천광역시":"인천","대전광역시":"대전",
        "대구광역시":"대구","광주광역시":"광주","울산광역시":"울산","세종특별자치시":"세종",
        "경기도":"경기","강원도":"강원","강원특별자치도":"강원",
        "충청북도":"충북","충청남도":"충남",
        "전라북도":"전북","전북특별자치도":"전북","전라남도":"전남",
        "경상북도":"경북","경상남도":"경남","제주특별자치도":"제주"
    }
    return mapping.get(s, s)

def get_parking_sido(df_parking):
    # 헤더 정리
    df_parking = df_parking.rename(columns={c: str(c).strip() for c in df_parking.columns})
    # 시도 컬럼 자동 탐색
    candidates = ["시도명", "CTPRVN_NM", "광역시도명", "시도", "SIDO"]
    col = next((c for c in candidates if c in df_parking.columns), None)
    if col is None:
        raise KeyError(f"시도 컬럼을 찾지 못했습니다. 현재 컬럼들: {list(df_parking.columns)}")
    if col != "시도명":
        df_parking = df_parking.rename(columns={col: "시도명"})
    # 표준화
    df_parking["시도명"] = (
        df_parking["시도명"].astype(str).str.strip().replace({
            "강원특별자치도":"강원","세종특별자치시":"세종",
            "전북특별자치도":"전북","제주특별자치도":"제주"
        })
    )
    return (df_parking.groupby("시도명", as_index=False)
            .size().rename(columns={"size":"공영주차장수"}))

# ===== 충남 분석용 유틸 =====
def normalize_region(s: str) -> str:
    if pd.isna(s): return s
    s = str(s)
    s = unicodedata.normalize('NFKC', s)
    s = s.replace('\u3000', ' ')
    s = s.strip()
    for ch in ['(', '（']:
        if ch in s:
            s = s.split(ch)[0].strip()
    s = s.replace(' ', '')
    return s

def coerce_numeric(series: pd.Series) -> pd.Series:
    # "12,345" → 12345, 공백/문자 coerce
    return pd.to_numeric(series.astype(str).str.replace(',', ''), errors="coerce")

def filter_chungnam(df_parking) -> pd.DataFrame:
    # 주차장 df에서 '충남'만 필터 (CTPRVN_NM/시도명 자동 인식)
    cols = df_parking.columns
    sido_col = "시도명" if "시도명" in cols else ("CTPRVN_NM" if "CTPRVN_NM" in cols else None)
    if sido_col is None:
        raise KeyError("충남 필터를 위해 '시도명' 또는 'CTPRVN_NM' 컬럼이 필요합니다.")
    s = df_parking[sido_col].astype(str).str.strip()
    mask = s.isin(["충남", "충청남도"])
    return df_parking[mask].copy()

def pick_sigungu_col(df: pd.DataFrame) -> str:
    # 시군구 컬럼 자동 선택
    for c in ["시군구명", "SIGNGU_NM", "시군구", "SIGUNGU"]:
        if c in df.columns: return c
    raise KeyError(f"시군구 컬럼을 찾지 못했습니다. 현재 컬럼들: {list(df.columns)}")

def percentile_rank(series: pd.Series, v: float) -> float:
    s = series.dropna()
    return float((s < v).sum() / len(s) * 100) if len(s) else np.nan
