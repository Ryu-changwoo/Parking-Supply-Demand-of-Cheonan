import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils_common import find_region_col_pop, latest_total_month_col, to_short_sido, get_parking_sido

df_pop = pd.read_csv("data/전국_시군구_성별인구수.csv", encoding="cp949")
df_parking = pd.read_csv("data/전국_시군구_공영주차장.csv")

# df_parking 읽은 직후
if "시도명" not in df_parking.columns and "CTPRVN_NM" in df_parking.columns:
    df_parking.rename(columns={"CTPRVN_NM": "시도명"}, inplace=True)

# 값 표준화(특별자치도 표기)
df_parking["시도명"] = (
    df_parking["시도명"].astype(str).str.strip().replace({
        "강원특별자치도": "강원",
        "세종특별자치시": "세종",
        "전북특별자치도": "전북",
        "제주특별자치도": "제주"
    })
)


# 인구 데이터
pop_region = find_region_col_pop(df_pop)
latest_col = latest_total_month_col(df_pop)
pop = df_pop[[pop_region, latest_col]].copy()
exclude = {"전국", pop_region, "합계", "소계"}
pop = pop[~pop[pop_region].isin(exclude)]
pop = pop[~pop[pop_region].str.contains(" ", na=False)]
pop["시도명"] = pop[pop_region].map(to_short_sido)
pop["인구"] = pd.to_numeric(pop[latest_col], errors="coerce")
pop_sido = pop.groupby("시도명", as_index=False)["인구"].sum()

# 주차장 데이터
parking_sido = get_parking_sido(df_parking)

# 결합 및 보급률
df_sido = pop_sido.merge(parking_sido, on="시도명", how="left")
df_sido["공영주차장수"] = pd.to_numeric(df_sido["공영주차장수"], errors="coerce").fillna(0).astype(int)
df_sido["보급률_10만명당"] = (df_sido["공영주차장수"] / df_sido["인구"]) * 100_000
df_plot = df_sido.sort_values("보급률_10만명당", ascending=False).reset_index(drop=True)
x_order = df_plot["시도명"].tolist()
mean_rate = df_plot["보급률_10만명당"].mean()

# 그래프
highlight = "충남"
bar_colors = ["#E45756" if s == highlight else "#4C78A8" for s in df_plot["시도명"]]

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(go.Bar(
    x=df_plot["시도명"], y=df_plot["공영주차장수"],
    text=df_plot["공영주차장수"].map(lambda v: f"{int(v):,}곳"),
    textposition="outside",
    marker=dict(color=bar_colors),
    name="공영주차장 수"
), secondary_y=False)

fig.add_trace(go.Scatter(
    x=df_plot["시도명"], y=df_plot["보급률_10만명당"],
    mode="lines+markers", name="보급률(10만명당)",
    hovertemplate="시도=%{x}<br>보급률=%{y:.2f}<extra></extra>"
), secondary_y=True)

fig.add_trace(go.Scatter(
    x=df_plot["시도명"], y=[mean_rate]*len(df_plot),
    mode="lines", line=dict(dash="dash"),
    name="평균 보급률",
    hovertemplate="평균 보급률: %{y:.2f}<extra></extra>"
), secondary_y=True)

fig.update_layout(
    title="시도별 공영주차장 수 & 인구 대비 보급률(10만명당)",
    xaxis_tickangle=-20,
    xaxis=dict(categoryorder="array", categoryarray=x_order),
    bargap=0.20
)

fig.show()