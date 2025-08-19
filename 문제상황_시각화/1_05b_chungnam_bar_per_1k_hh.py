import pandas as pd
import numpy as np
import plotly.express as px

df = pd.read_csv("output/chungnam_metrics.csv", encoding="utf-8-sig")
df["is_cheonan"] = df["시군구명"].astype(str).str.contains("천안")
df["label"] = np.where(df["is_cheonan"], df["시군구명"] + " ⭐", df["시군구명"])

df_plot = df.sort_values("세대1000가구당_주차시설", ascending=False).copy()
colors = np.where(df_plot["is_cheonan"], "crimson", "steelblue")

fig = px.bar(
    df_plot, x="label", y="세대1000가구당_주차시설_r",
    title="충남 시군구별 세대 1,000가구당 공영주차장 시설 수",
    labels={"label":"시군구","세대1000가구당_주차시설_r":"시설/1,000가구"},
)
fig.update_traces(marker_color=colors,
                  hovertemplate="<b>%{x}</b><br>세대 1,000가구당: %{y}<extra></extra>")
fig.update_layout(
    xaxis={"categoryorder":"array","categoryarray":list(df_plot["label"])},
    yaxis_title="시설/1,000가구", xaxis_title="시군구", title_x=0.5
)
fig.show()
