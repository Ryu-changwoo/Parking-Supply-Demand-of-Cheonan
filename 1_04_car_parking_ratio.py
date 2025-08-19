import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils_common import get_parking_sido

df_car = pd.read_csv("data/전국_시군구_차량등록대수.csv", encoding="cp949")
df_parking = pd.read_csv("data/전국_시군구_공영주차장.csv")

df_car = df_car[(df_car["시군구"]=="계") & (df_car["레벨01"]=="총계")]
df_car_sido = df_car[["시도명","계"]].rename(columns={"계":"총계"})
df_car_sido["총계"] = pd.to_numeric(df_car_sido["총계"], errors="coerce")

parking_sido = get_parking_sido(df_parking)
df_sido = df_car_sido.merge(parking_sido, on="시도명", how="left")
df_sido["공영주차장수"] = pd.to_numeric(df_sido["공영주차장수"], errors="coerce").fillna(0).astype(int)
df_sido["보급률_1만대당"] = (df_sido["공영주차장수"] / df_sido["총계"]) * 10_000

df_plot = df_sido.sort_values("보급률_1만대당", ascending=False).reset_index(drop=True)
x_order = df_plot["시도명"].tolist()
mean_rate = df_plot["보급률_1만대당"].mean()

highlight = "충남"
bar_colors = ["#E45756" if s==highlight else "#4C78A8" for s in df_plot["시도명"]]

fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(go.Bar(
    x=df_plot["시도명"], y=df_plot["공영주차장수"],
    text=df_plot["공영주차장수"].map(lambda v: f"{int(v):,}곳"),
    textposition="outside", marker=dict(color=bar_colors),
    name="공영주차장 수"
), secondary_y=False)

fig.add_trace(go.Scatter(
    x=df_plot["시도명"], y=df_plot["보급률_1만대당"],
    mode="lines+markers", name="보급률(1만대당)"
), secondary_y=True)

fig.add_trace(go.Scatter(
    x=df_plot["시도명"], y=[mean_rate]*len(df_plot),
    mode="lines", line=dict(dash="dash"), name="평균 보급률"
), secondary_y=True)

fig.update_layout(
    title="시도별 공영주차장 수 & 차량 대비 보급률(1만대당)",
    xaxis_tickangle=-20,
    xaxis=dict(categoryorder="array", categoryarray=x_order),
    bargap=0.20
)

fig.show()