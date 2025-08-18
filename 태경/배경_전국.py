import pandas as pd
import numpy as np


'''
데이터 세팅
'''
df_pop = pd.read_csv('전국_시군구_성별인구수.csv', encoding='cp949')
df_house = pd.read_csv('전국_시군구_주민등록세대수.csv', encoding='cp949')
df_car = pd.read_csv('전국_시군구_차량등록대수.csv', encoding='cp949')
df_parking = pd.read_csv('전국_시군구_공영주차장.csv')

df_chungnam_pop = pd.read_csv('충남_인구_세대.csv', encoding='cp949')
df_chungnam_parking = pd.read_csv('충청남도_천안시_공영주차장정보.csv', encoding='cp949')

# df_parking 컬럼명 변경
df_parking.rename(columns={
    "ID": "ID",
    "LCLAS_NM": "대분류명",
    "MLSFC_NM": "중분류명",
    "FCLTY_NM": "시설명",
    "CTPRVN_NM": "시도명",
    "SIGNGU_NM": "시군구명",
    "LEGALDONG_CD": "법정동코드",
    "LEGALDONG_NM": "법정동명",
    "ADSTRD_CD": "행정동코드",
    "ADSTRD_NM": "행정동명",
    "RDNMADR_CD": "도로명주소코드",
    "RDNMADR_NM": "도로명주소명",
    "ZIP_NO": "우편번호",
    "GID_CD": "격자코드",
    "FCLTY_LO": "시설경도",
    "FCLTY_LA": "시설위도",
    "MANAGE_NO": "관리번호",
    "FLAG_NM": "구분명",
    "TY_NM": "유형명",
    "PARKNG_SPCE_CO": "주차공간수",
    "MANAGE_FLAG_NM": "관리구분명",
    "UTILIIZA_LMTT_FLAG_NM": "이용제한구분명",
    "WKDAY_NM": "요일명",
    "WORKDAY_OPN_BSNS_TIME": "평일개점시간",
    "WORKDAY_CLOS_TIME": "평일마감시간",
    "SAT_OPN_BSNS_TIME": "토요일개점시간",
    "SAT_CLOS_TIME": "토요일마감시간",
    "SUN_OPN_BSNS_TIME": "일요일개점시간",
    "SUN_CLOS_TIME": "일요일마감시간",
    "UTILIIZA_CHRGE_CN": "이용요금내용",
    "BASS_TIME": "기본시간",
    "BASS_PRICE": "기본금액",
    "ADIT_UNIT_TIME": "추가단위시간",
    "ADIT_UNIT_PRICE": "추가단위금액",
    "ONE_DAY_PARKNG_VLM_TIME": "1일주차권시간",
    "ONE_DAY_PARKNG_VLM_PRICE": "1일주차권금액",
    "MT_FDRM_PRICE": "월정기금액",
    "SETLE_MTH_CN": "결제방법내용",
    "ADIT_DC": "추가설명",
    "MANAGE_INSTT_NM": "관리기관명",
    "TEL_NO": "전화번호",
    "데이터기준일자": "데이터기준일자",
    "PROVD_INSTT_CD": "제공기관코드",
    "PROVD_INSTT_NM": "제공기관명",
    "LAST_CHG_DE": "최종변경일자",
    "ORIGIN_NM": "출처명",
    "FILE_NM": "파일명",
    "BASE_DE": "기준일자"
}, inplace=True)

df_parking["시도명"] = df_parking["시도명"].replace({
    "강원특별자치도": "강원",
    "세종특별자치시": "세종",
    "전북특별자치도": "전북",
    "제주특별자치도": "제주"
})

df_parking.head()
df_parking["시도명"].value_counts()





'''
시도별 인구 수 시각화
'''

import re
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 0) 도우미: 인구 테이블의 지역/최신열 찾기 + 시도명 짧은표기 정규화
def find_region_col_pop(df):
    for c in df.columns:
        if ("행정구역" in c) or ("시도명" in c) or ("시군구명" in c):
            return c
    raise KeyError("인구 테이블에서 지역 열을 찾지 못했습니다.")

def latest_total_month_col(df):
    # KOSIS: 'YYYY.MM'(계), 'YYYY.MM.1'(남), 'YYYY.MM.2'(여)
    month_cols = [c for c in df.columns if re.fullmatch(r"\d{4}\.\d{2}", str(c))]
    if not month_cols:
        # 접두만 추출해도 됨
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
        "대구광역시":"대구","광주광역시":"광주","울산광역시":"울산",
        "세종특별자치시":"세종",
        "경기도":"경기",
        "강원도":"강원","강원특별자치도":"강원",
        "충청북도":"충북","충청남도":"충남",
        "전라북도":"전북","전북특별자치도":"전북",
        "전라남도":"전남",
        "경상북도":"경북","경상남도":"경남",
        "제주특별자치도":"제주"
    }
    return mapping.get(s, s)

# 1) 시도별 인구 집계 (df_pop 사용)
pop_region = find_region_col_pop(df_pop)
latest_col = latest_total_month_col(df_pop)

pop = df_pop[[pop_region, latest_col]].copy()
pop[pop_region] = pop[pop_region].astype(str).str.strip()

# 불필요 행 제거: '전국', 컬럼명 그 자체, 합계/소계
exclude = { "전국", pop_region, "합계", "소계" }
pop = pop[~pop[pop_region].isin(exclude)]

# 시도 레벨만: 공백 포함(구/군 있는 행) 제외
pop = pop[~pop[pop_region].str.contains(" ", na=False)]

# 짧은 시도명으로 통일
pop["시도명"] = pop[pop_region].map(to_short_sido)

# 인구 숫자형
pop["인구"] = pd.to_numeric(pop[latest_col], errors="coerce")
pop = pop.dropna(subset=["인구"])

pop_sido = pop.groupby("시도명", as_index=False)["인구"].sum()

# 2) 시도별 공영주차장 수 집계 (df_parking 사용)
parking_sido = (
    df_parking.groupby("시도명", as_index=False).size()
              .rename(columns={"size":"공영주차장수"})
)

# 3) 결합 + 숫자형 보정 + 보급률(10만명당)
df_sido = pop_sido.merge(parking_sido, on="시도명", how="left")
df_sido["공영주차장수"] = pd.to_numeric(df_sido["공영주차장수"], errors="coerce").fillna(0).astype(int)
df_sido["인구"] = pd.to_numeric(df_sido["인구"], errors="coerce")
df_sido["보급률_10만명당"] = (df_sido["공영주차장수"] / df_sido["인구"]) * 100_000

# 4) 그래프 ①: 시도별 인구 (내림차순)
rank_pop = df_sido.sort_values("인구", ascending=False)

highlight = "충남"
bar_default = "#4C78A8"   # 2번 그래프의 기본 막대 색
bar_highlight = "#E45756" # 2번 그래프의 충남 강조 색
bar_colors_pop = [bar_highlight if s == highlight else bar_default for s in rank_pop["시도명"]]

fig_pop = go.Figure(
    go.Bar(
        x=rank_pop["시도명"],
        y=rank_pop["인구"],
        text=rank_pop["인구"].map(lambda v: f"{int(v):,}"),
        textposition="outside",
        marker=dict(color=bar_colors_pop),
        hovertemplate="시도=%{x}<br>인구=%{y:,}명<extra></extra>"
    )
)
x_order = df_sido.sort_values("인구", ascending=False)["시도명"].tolist()
n = len(x_order)

fig_pop.update_traces(textposition="outside")
fig_pop.update_layout(
    title=f"시도별 인구 수 (기준: {latest_col})",
    xaxis_tickangle=-20,
    yaxis_title="인구 수",
    xaxis_title="시도명",
    bargap=0.20,  # 2번과 동일 간격
    margin=dict(l=10, r=50, t=60, b=10)
)
# 카테고리 순서 + 양끝 패딩(±0.5가 기본 반칸; 더 여유 원하면 0.6/0.7로)
fig_pop.update_xaxes(
    type="category",
    categoryorder="array",
    categoryarray=x_order,
    range=[-1, n ]   # ← 좌우에 반칸씩 여백
)

fig_pop.show()





'''
시도별 인구 수 대비 공영주차장 보급률 시각화
'''

# 5) 그래프 ②: 공영주차장 수(막대, 좌) + 보급률(선, 우; 10만명당)
# ----- x축을 인구수 내림차순으로 고정 + 평균 보급률 점선 + 충남 강조 -----
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# 1) 인구수 내림차순 시도 순서
x_order = df_sido.sort_values("인구", ascending=False)["시도명"].tolist()

# 2) plotting용 DF (그 순서대로)
df_plot = df_sido.set_index("시도명").loc[x_order].reset_index()

# 3) 전체 평균 보급률(10만명당)
mean_rate = df_sido["보급률_10만명당"].mean()

# 4) 색상: 충남만 강조
highlight = "충남"
bar_default = "#4C78A8"
bar_highlight = "#E45756"
bar_colors = [bar_highlight if s == highlight else bar_default for s in df_plot["시도명"]]

fig2 = make_subplots(specs=[[{"secondary_y": True}]])

# (A) 공영주차장 수 (좌축, 막대) — 충남만 색상 강조
fig2.add_trace(
    go.Bar(
        x=df_plot["시도명"],
        y=df_plot["공영주차장수"],
        name="공영주차장 수",
        text=df_plot["공영주차장수"].map(lambda v: f"{int(v):,}곳"),
        textposition="outside",
        marker=dict(color=bar_colors),
        hovertemplate="시도=%{x}<br>주차장=%{y:,}곳<extra></extra>",
    ),
    secondary_y=False
)

# (B) 보급률(10만명당) (우축, 선)
fig2.add_trace(
    go.Scatter(
        x=df_plot["시도명"],
        y=df_plot["보급률_10만명당"],
        name="보급률",
        mode="lines+markers",
        hovertemplate="시도=%{x}<br>보급률=%{y:.2f}<extra></extra>",
    ),
    secondary_y=True
)

# (C) 평균 보급률 점선 — 양 끝까지(첫 시도 ↔ 마지막 시도) 연결
fig2.add_trace(
    go.Scatter(
        x=df_plot["시도명"],
        y=[mean_rate] * len(df_plot),
        name="평균 보급률",
        mode="lines",
        line=dict(dash="dash"),
        hovertemplate="평균 보급률: %{y:.2f}<extra></extra>",
    ),
    secondary_y=True
)

# (D) 보급률 선에서 '충남' 포인트만 별도 강조(마커 크게/색상 변경)
if highlight in df_plot["시도명"].values:
    y_hl = df_plot.loc[df_plot["시도명"] == highlight, "보급률_10만명당"].iloc[0]
    fig2.add_trace(
        go.Scatter(
            x=[highlight], y=[y_hl],
            name="충남",
            mode="markers",
            marker=dict(size=12, color=bar_highlight, line=dict(width=1, color="#333")),
            hovertemplate="시도=%{x}<br>보급률=%{y:.2f}<extra></extra>",
            showlegend=False
        ),
        secondary_y=True
    )

# 레이아웃: x축 순서 고정
fig2.update_layout(
    title_text="시도별 공영주차장 수 & 인구 대비 보급률(10만명당)",
    xaxis_tickangle=-20,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
    xaxis=dict(categoryorder="array", categoryarray=x_order),
    margin=dict(l=10, r=10, t=60, b=10)
)
fig2.update_yaxes(title_text="공영주차장 수(곳)", secondary_y=False)
fig2.update_yaxes(title_text="보급률(10만명당)", secondary_y=True)

fig2.show()














'''
시도별 차량등록대수 시각화
'''

df_car = pd.read_csv('전국_시군구_차량등록대수.csv', encoding='cp949')
# 공백 제거
df_car['시도명'] = df_car['시도명'].astype(str).str.strip()
df_car['시군구'] = df_car['시군구'].astype(str).str.strip()
df_car['레벨01'] = df_car['레벨01'].astype(str).str.strip()
# 조건 필터: 시군구=계, 레벨01=총계
df_car_sido = df_car[(df_car['시군구'] == '계') & (df_car['레벨01'] == '총계')]
# 숫자 변환
df_car_sido['계'] = pd.to_numeric(df_car_sido['계'], errors='coerce')
# 시도명 + 총계 컬럼만 선택, 컬럼명 변경
df_car_sido = df_car_sido[['시도명', '계']].rename(columns={'계': '총계'})
df_car_sido = df_car_sido.reset_index(drop=True)
df_car_sido


from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

df_car_sido2 = df_car_sido.copy()
df_car_sido2["총계"] = pd.to_numeric(df_car_sido2["총계"], errors="coerce")

# 차량등록대수 내림차순
rank_car = df_car_sido2.sort_values("총계", ascending=False)

# 색상 (충남 강조)
highlight = "충남"
bar_default = "#4C78A8"
bar_highlight = "#E45756"
bar_colors_car = [bar_highlight if s == highlight else bar_default for s in rank_car["시도명"]]

# 그래프
fig_car_total = go.Figure(
    go.Bar(
        x=rank_car["시도명"],
        y=rank_car["총계"],
        text=rank_car["총계"].map(lambda v: f"{int(v):,}대"),
        textposition="outside",
        marker=dict(color=bar_colors_car),
        hovertemplate="시도=%{x}<br>차량등록대수=%{y:,}대<extra></extra>"
    )
)

# 2번 그래프와 동일한 x축 순서/간격/패딩 적용
x_order = rank_car["시도명"].tolist()
n = len(x_order)

fig_car_total.update_traces(textposition="outside")
fig_car_total.update_layout(
    title="전국 시도별 차량등록대수 (기준: 2025.06)",
    xaxis_tickangle=-20,
    yaxis_title="차량등록대수(대)",
    xaxis_title="시도명",
    bargap=0.20,  # 2번 그래프와 동일 간격
    margin=dict(l=10, r=50, t=60, b=10)
)
# 카테고리 순서 고정 + 양끝 패딩(여백 더 주고 싶으면 [-1.1, n+0.1] 등 조절)
fig_car_total.update_xaxes(
    type="category",
    categoryorder="array",
    categoryarray=x_order,
    range=[-1, n]
)

fig_car_total.show()






'''
시도별 차량등록대수 대비 공영주차장 보급률 시각화
'''

# --- 0) 준비: df_car_sido = [시도명, 총계], df_parking = 원본(시도명 정리됨) ---
# 1) 시도별 공영주차장 수 집계
parking_sido = (
    df_parking.groupby("시도명", as_index=False)
              .size()
              .rename(columns={"size": "공영주차장수"})
)

# 2) 차량등록대수(총계)와 결합
df_car_sido2 = df_car_sido.copy()
df_car_sido2["총계"] = pd.to_numeric(df_car_sido2["총계"], errors="coerce")

df_sido_car = df_car_sido2.merge(parking_sido, on="시도명", how="left")
df_sido_car["공영주차장수"] = pd.to_numeric(df_sido_car["공영주차장수"], errors="coerce").fillna(0).astype(int)

# 3) 보급률: 차량 1만대당 주차장 수
df_sido_car["보급률_1만대당"] = (df_sido_car["공영주차장수"] / df_sido_car["총계"]) * 10_000

# 4) x축 순서: 차량등록대수 내림차순
x_order = df_sido_car.sort_values("총계", ascending=False)["시도명"].tolist()
df_plot = df_sido_car.set_index("시도명").loc[x_order].reset_index()

# 5) 평균 보급률(1만대당) & 색상(충남 강조)
mean_rate = df_plot["보급률_1만대당"].mean()
highlight = "충남"
bar_default = "#4C78A8"
bar_highlight = "#E45756"
bar_colors = [bar_highlight if s == highlight else bar_default for s in df_plot["시도명"]]

# 6) 그래프: 공영주차장 수(막대, 좌) + 보급률(선, 우; 1만대당)
fig_car = make_subplots(specs=[[{"secondary_y": True}]])

# (A) 주차장 수 막대
fig_car.add_trace(
    go.Bar(
        x=df_plot["시도명"],
        y=df_plot["공영주차장수"],
        name="공영주차장 수",
        text=df_plot["공영주차장수"].map(lambda v: f"{int(v):,}곳"),
        textposition="outside",
        marker=dict(color=bar_colors),
        hovertemplate="시도=%{x}<br>주차장=%{y:,}곳<extra></extra>",
    ),
    secondary_y=False
)

# (B) 보급률(1만대당) 선
fig_car.add_trace(
    go.Scatter(
        x=df_plot["시도명"],
        y=df_plot["보급률_1만대당"],
        name="보급률(1만대당)",
        mode="lines+markers",
        hovertemplate="시도=%{x}<br>보급률=%{y:.2f}<extra></extra>",
    ),
    secondary_y=True
)

# (C) 평균 보급률 점선 — 모든 시도에 대해 호버 표시
fig_car.add_trace(
    go.Scatter(
        x=df_plot["시도명"],
        y=[mean_rate] * len(df_plot),
        name="평균 보급률",
        mode="lines",
        line=dict(dash="dash"),
        hovertemplate="평균 보급률: %{y:.2f}<extra></extra>",
    ),
    secondary_y=True
)

# (D) 충남 포인트만 추가 강조(범례 제외)
if highlight in df_plot["시도명"].values:
    y_hl = df_plot.loc[df_plot["시도명"] == highlight, "보급률_1만대당"].iloc[0]
    fig_car.add_trace(
        go.Scatter(
            x=[highlight], y=[y_hl],
            name="충남(보급률 강조)",
            mode="markers",
            marker=dict(size=12, color=bar_highlight, line=dict(width=1, color="#333")),
            hovertemplate="시도=%{x}<br>보급률=%{y:.2f}<extra></extra>",
            showlegend=False
        ),
        secondary_y=True
    )

# 7) 레이아웃
fig_car.update_layout(
    title_text="시도별 공영주차장 수 & 차량 대비 보급률(1만대당)",
    xaxis_tickangle=-20,
    xaxis=dict(categoryorder="array", categoryarray=x_order),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                font=dict(size=10)),
    margin=dict(l=10, r=10, t=60, b=10)
)
fig_car.update_yaxes(title_text="공영주차장 수(곳)", secondary_y=False)
fig_car.update_yaxes(title_text="보급률(1만대당)", secondary_y=True)

fig_car.show()