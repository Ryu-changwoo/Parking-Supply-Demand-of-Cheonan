import pandas as pd
import plotly.graph_objects as go

df_car = pd.read_csv("data/전국_시군구_차량등록대수.csv", encoding="cp949")
df_car = df_car[(df_car["시군구"]=="계") & (df_car["레벨01"]=="총계")]
df_car_sido = df_car[["시도명","계"]].rename(columns={"계":"총계"})
df_car_sido["총계"] = pd.to_numeric(df_car_sido["총계"], errors="coerce")

rank_car = df_car_sido.sort_values("총계", ascending=False)
highlight = "충남"
bar_colors = ["#E45756" if s==highlight else "#4C78A8" for s in rank_car["시도명"]]

fig = go.Figure(go.Bar(
    x=rank_car["시도명"],
    y=rank_car["총계"],
    text=rank_car["총계"].map(lambda v: f"{int(v):,}대"),
    textposition="outside",
    marker=dict(color=bar_colors),
    hovertemplate="시도=%{x}<br>차량등록대수=%{y:,}대<extra></extra>"
))

fig.update_layout(
    title="전국 시도별 차량등록대수",
    xaxis_tickangle=-20,
    yaxis_title="차량등록대수(대)",
    bargap=0.20
)

fig.show()
