import pandas as pd
import numpy as np
import plotly.express as px

df = pd.read_csv("output/chungnam_metrics.csv", encoding="utf-8-sig")
df["is_cheonan"] = df["시군구명"].astype(str).str.contains("천안")

df["인구1만명당_주차시설_r"]   = df["인구1만명당_주차시설"].round(3)
df["세대1000가구당_주차시설_r"] = df["세대1000가구당_주차시설"].round(3)

cheonan_only = df[df["is_cheonan"]].copy()

cheonan_long = pd.melt(
    cheonan_only,
    id_vars=["시군구명"],
    value_vars=["인구1만명당_주차시설_r","세대1000가구당_주차시설_r"],
    var_name="지표", value_name="값"
)
cheonan_long["지표"] = cheonan_long["지표"].map({
    "인구1만명당_주차시설_r":"인구 1만명당",
    "세대1000가구당_주차시설_r":"세대 1,000가구당"
})

fig = px.bar(
    cheonan_long, x="시군구명", y="값", color="지표",
    barmode="group",
    title="천안 동남구 vs 서북구 주차시설 지표 비교",
    labels={"시군구명":"시군구","값":"값","지표":"지표"}
)
fig.update_layout(title_x=0.5)
fig.show()
