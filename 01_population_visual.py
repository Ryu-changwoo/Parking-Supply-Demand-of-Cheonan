import pandas as pd
import plotly.graph_objects as go
from utils_common import find_region_col_pop, latest_total_month_col, to_short_sido

df_pop = pd.read_csv("data/전국_시군구_성별인구수.csv", encoding="cp949")

# 시도별 인구 집계
pop_region = find_region_col_pop(df_pop)
latest_col = latest_total_month_col(df_pop)
pop = df_pop[[pop_region, latest_col]].copy()
exclude = { "전국", pop_region, "합계", "소계" }
pop = pop[~pop[pop_region].isin(exclude)]
pop = pop[~pop[pop_region].str.contains(" ", na=False)]
pop["시도명"] = pop[pop_region].map(to_short_sido)
pop["인구"] = pd.to_numeric(pop[latest_col], errors="coerce")
pop_sido = pop.groupby("시도명", as_index=False)["인구"].sum()

# 그래프
rank_pop = pop_sido.sort_values("인구", ascending=False)
highlight = "충남"
bar_colors = ["#E45756" if s == highlight else "#4C78A8" for s in rank_pop["시도명"]]

fig = go.Figure(go.Bar(
    x=rank_pop["시도명"],
    y=rank_pop["인구"],
    text=rank_pop["인구"].map(lambda v: f"{int(v):,}"),
    textposition="outside",
    marker=dict(color=bar_colors),
    hovertemplate="시도=%{x}<br>인구=%{y:,}명<extra></extra>"
))

fig.update_layout(
    title=f"시도별 인구 수 (기준: {latest_col})",
    xaxis_tickangle=-20,
    yaxis_title="인구 수",
    bargap=0.20
)

fig.show()
