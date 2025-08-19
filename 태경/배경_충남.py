import ccxt
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

'''
충청남도 시군구별
인구/세대 vs 공영주차장
'''
df_car = pd.read_csv('전국_시군구_차량등록대수.csv', encoding='cp949')
df_parking = pd.read_csv('전국_시군구_공영주차장.csv')
df_pop = pd.read_csv('충남_인구_세대.csv', encoding='cp949')
df_parking
df_pop

import pandas as pd
import numpy as np
import unicodedata


'''
1) 인구 데이터 전처리
남성 + 여성 인구수 합
'''

pop = df_pop.copy()
# 총인구 계산(남+여)
if {'남성인구수','여성인구수'}.issubset(pop.columns):
    pop['총인구'] = pop['남성인구수'].astype(float) + pop['여성인구수'].astype(float)
else:
    raise ValueError("인구 데이터에 '남성인구수', '여성인구수' 컬럼이 필요합니다.")


'''
2) 주차장 데이터에서 충남만 필터
전국 데이터에서 충남, 충청남도 데이터만 필터
'''

park = df_parking.copy()
# 시도명 표기가 '충남' or '충청남도'일 수 있어 둘 다 허용
mask_chungnam = park['CTPRVN_NM'].isin(['충남','충청남도'])
park_chungnam = park[mask_chungnam].copy()


'''
3) 지역명 정규화 함수
'''

def normalize_region(s: str) -> str:
    if pd.isna(s): 
        return s
    s = str(s)
    s = unicodedata.normalize('NFKC', s)  # 유니코드 정규화
    s = s.replace('\u3000', ' ')          # 전각 공백 → 일반 공백
    s = s.strip()
    # 괄호 설명 제거: "천안시 동남구(원도심)" → "천안시 동남구"
    for ch in ['(', '（']:
        if ch in s:
            s = s.split(ch)[0].strip()
    # 완전매칭을 위해 공백 제거 버전 사용
    s = s.replace(' ', '')
    return s


'''
4) 매칭 키 생성
'''

if '시군구명' not in pop.columns:
    raise ValueError("인구 데이터에 '시군구명' 컬럼이 필요합니다.")
if 'SIGNGU_NM' not in park_chungnam.columns:
    raise ValueError("주차장 데이터에 'SIGNGU_NM' 컬럼이 필요합니다.")

pop['키'] = pop['시군구명'].apply(normalize_region)
park_chungnam['키'] = park_chungnam['SIGNGU_NM'].apply(normalize_region)


'''
5) 주차장 공급(시설수) 집계
'''

park_count = (
    park_chungnam
    .groupby('키', as_index=False)
    .size()
    .rename(columns={'size': '공영주차장_시설수'})
)


'''
6) 병합
'''

df = pop.merge(park_count, on='키', how='left')
df['공영주차장_시설수'] = df['공영주차장_시설수'].fillna(0).astype(int)


'''
7) 지표 산출 기준
'''

# 인구/세대당 값: 클수록 '공급 여유', 작을수록 '부족'
df['인구1만명당_주차시설'] = df['공영주차장_시설수'] / (df['총인구'] / 10_000).replace(0, np.nan)
df['세대1000가구당_주차시설'] = df['공영주차장_시설수'] / (df['세대수'] / 1_000).replace(0, np.nan)


'''
8) 천안 요약
'''

cheonan_mask = df['시군구명'].astype(str).str.contains('천안')
cheonan = df[cheonan_mask].copy()

def percentile_rank(series, v):
    series = series.dropna()
    return float((series < v).sum() / len(series) * 100)

summary_rows = []
for _, r in cheonan.iterrows():
    pr_pop = percentile_rank(df['인구1만명당_주차시설'], r['인구1만명당_주차시설'])
    pr_hh  = percentile_rank(df['세대1000가구당_주차시설'], r['세대1000가구당_주차시설'])
    summary_rows.append({
        '시군구명': r['시군구명'],
        '총인구': int(r['총인구']),
        '세대수': int(r['세대수']),
        '공영주차장_시설수': int(r['공영주차장_시설수']),
        '인구1만명당_주차시설': round(r['인구1만명당_주차시설'], 3),
        '세대1000가구당_주차시설': round(r['세대1000가구당_주차시설'], 3),
        '인구1만명당_백분위(여유)': round(pr_pop, 1),
        '세대1000가구당_백분위(여유)': round(pr_hh, 1)
    })
cheonan_summary = pd.DataFrame(summary_rows)


'''
9) 부족 지역 우선 확인용 정렬 표
'''

result_sorted = df.sort_values(
    ['인구1만명당_주차시설', '세대1000가구당_주차시설'],
    ascending=[True, True]
)[['기준연도','시군구명','총인구','세대수','공영주차장_시설수','인구1만명당_주차시설','세대1000가구당_주차시설']]


'''
10) 출력(프린트/미리보기)
'''

print("\n[천안 요약]")
print(cheonan_summary.to_string(index=False))

print("\n[충남 시군구별 지표 (부족지역 우선 정렬, 상위 10)]")
print(result_sorted.head(10).to_string(index=False))


'''
ployly 시각화
'''

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# ===== 준비: 천안 하이라이트 / 정렬 기준 =====
viz = df.copy()
viz['is_cheonan'] = viz['시군구명'].astype(str).str.contains('천안')
viz['label'] = np.where(viz['is_cheonan'], viz['시군구명'] + " ⭐", viz['시군구명'])

# 보기 좋게 소수점 반올림
viz['인구1만명당_주차시설_r'] = viz['인구1만명당_주차시설'].round(3)
viz['세대1000가구당_주차시설_r'] = viz['세대1000가구당_주차시설'].round(3)

# 정렬 인덱스(부족→여유 순)
order_pop = viz.sort_values('인구1만명당_주차시설', ascending=True)['label']
order_hh  = viz.sort_values('세대1000가구당_주차시설', ascending=True)['label']

# 색상: 천안 강조
colors = np.where(viz['is_cheonan'], 'crimson', 'steelblue')


'''
1) 인구 1만명당 주차시설 막대그래프 =====
'''
if 'label' not in viz.columns:
    viz['label'] = np.where(viz['is_cheonan'], viz['시군구명'] + " ⭐", viz['시군구명'])

df_plot1 = viz.sort_values('인구1만명당_주차시설', ascending=False).copy()
colors1 = np.where(df_plot1['is_cheonan'], 'crimson', 'steelblue')

fig1 = px.bar(
    df_plot1,
    x='label', y='인구1만명당_주차시설_r',
    title='충남 시군구별 인구 1만명당 공영주차장 시설 수',
    labels={'label':'시군구', '인구1만명당_주차시설_r':'인구 1만명당 주차시설'},
)
fig1.update_traces(
    marker_color=colors1,
    hovertemplate="<b>%{x}</b><br>인구 1만명당: %{y}<extra></extra>"
)
fig1.update_layout(
    xaxis={'categoryorder':'array','categoryarray':list(df_plot1['label'])},
    yaxis_title='시설/1만명', xaxis_title='시군구', title_x=0.5
)
fig1.show()


'''
2) 세대 1,000가구당 주차시설 막대그래프
'''

df_plot2 = viz.sort_values('세대1000가구당_주차시설', ascending=False).copy()
colors2 = np.where(df_plot2['is_cheonan'], 'crimson', 'steelblue')

fig2 = px.bar(
    df_plot2,
    x='label', y='세대1000가구당_주차시설_r',
    title='충남 시군구별 세대 1,000가구당 공영주차장 시설 수',
    labels={'label':'시군구', '세대1000가구당_주차시설_r':'세대 1,000가구당 주차시설'},
)
fig2.update_traces(
    marker_color=colors2,
    hovertemplate="<b>%{x}</b><br>세대 1,000가구당: %{y}<extra></extra>"
)
fig2.update_layout(
    xaxis={'categoryorder':'array','categoryarray':list(df_plot2['label'])},
    yaxis_title='시설/1,000가구', xaxis_title='시군구', title_x=0.5
)
fig2.show()


'''
3) 두 지표 비교 산점도 (천안 라벨 표시 + 기준선)
'''

import numpy as np
import plotly.express as px

viz = df.copy()
viz['is_cheonan'] = viz['시군구명'].astype(str).str.contains('천안')
viz['그룹'] = np.where(viz['is_cheonan'], '천안', '기타')

fig3 = px.scatter(
    viz,
    x='인구1만명당_주차시설', y='세대1000가구당_주차시설',
    color='그룹',
    hover_name='시군구명',
    labels={'인구1만명당_주차시설':'인구 1만명당', '세대1000가구당_주차시설':'세대 1,000가구당'},
    title='인구 vs 세대 기준 주차시설 비교(충남)'
)

cheonan_mask = viz['is_cheonan']
for tr in fig3.data:
    if tr.name == '천안':
        # 천안 점(동남구, 서북구)만 텍스트 표시 + 위쪽 배치
        tr.text = viz.loc[cheonan_mask, '시군구명']
        tr.textposition = 'top center'
        tr.mode = 'markers+text'          # <-- 중요!
        tr.marker.size = 14
        tr.textfont = dict(size=12)
        tr.cliponaxis = False             # 축 경계 넘어가도 라벨 보이게
    else:
        tr.text = None
        tr.mode = 'markers'
        tr.marker.size = 9

# 평균선
fig3.add_hline(y=viz['세대1000가구당_주차시설'].mean(), line_dash="dot")
fig3.add_vline(x=viz['인구1만명당_주차시설'].mean(), line_dash="dot")
fig3.update_layout(title_x=0.5)
fig3.show()


'''
4) 천안 두 구만 세부 비교 막대 (동남구 vs 서북구)
'''
viz = df.copy()
viz['is_cheonan'] = viz['시군구명'].astype(str).str.contains('천안')

viz['인구1만명당_주차시설_r'] = viz['인구1만명당_주차시설'].round(3)
viz['세대1000가구당_주차시설_r'] = viz['세대1000가구당_주차시설'].round(3)

cheonan_only = viz[viz['is_cheonan']].copy()

cheonan_long = pd.melt(
    cheonan_only,
    id_vars=['시군구명'],
    value_vars=['인구1만명당_주차시설_r', '세대1000가구당_주차시설_r'],
    var_name='지표', value_name='값'
)

cheonan_long['지표'] = cheonan_long['지표'].map({
    '인구1만명당_주차시설_r': '인구 1만명당',
    '세대1000가구당_주차시설_r': '세대 1,000가구당'
})

fig4 = px.bar(
    cheonan_long,
    x='시군구명', y='값', color='지표',
    barmode='group',
    title='천안 동남구 vs 서북구 주차시설 지표 비교',
    labels={'시군구명': '시군구', '값': '값', '지표': '지표'}
)
fig4.update_layout(title_x=0.5)
fig4.show()

