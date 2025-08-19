import pandas as pd
import numpy as np
import plotly.express as px

df = pd.read_csv("output/chungnam_metrics.csv", encoding="utf-8-sig")
df["is_cheonan"] = df["시군구명"].astype(str).str.contains("천안")
df["그룹"] = np.where(df["is_cheonan"], "천안", "기타")

fig = px.scatter(
    df,
    x="인구1만명당_주차시설", y="세대1000가구당_주차시설",
    color="그룹", hover_name="시군구명",
    labels={"인구1만명당_주차시설":"인구 1만명당","세대1000가구당_주차시설":"세대 1,000가구당"},
    title="인구 vs 세대 기준 주차시설 비교(충남)"
)

# 천안 마커 라벨/크기 강조
cheonan_mask = df["is_cheonan"]
for tr in fig.data:
    if tr.name == "천안":
        tr.text = df.loc[cheonan_mask, "시군구명"]
        tr.textposition = "top center"
        tr.mode = "markers+text"
        tr.marker.size = 14
        tr.textfont = dict(size=12)
        tr.cliponaxis = False
    else:
        tr.text = None
        tr.mode = "markers"
        tr.marker.size = 9

fig.add_hline(y=df["세대1000가구당_주차시설"].mean(), line_dash="dot")
fig.add_vline(x=df["인구1만명당_주차시설"].mean(), line_dash="dot")
fig.update_layout(title_x=0.5)
fig.show()
