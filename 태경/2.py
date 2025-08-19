import ccxt
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

'''
자동차등록대수현황_시도별
'''
df = pd.read_csv('전국_시군구_차량등록대수.csv', encoding="cp949")
df.head()
df.tail()
df.info()

df = df.drop(columns=['Unnamed: 8'])
df['시점'] = pd.to_datetime(df['시점'].str.replace(' 월', ''), format='%Y.%m')
df = df.rename(columns={'레벨01': '차량구분'})
df
df['시도명'].unique()

import pandas as pd
import plotly.express as px

'''
서울 등록대수
'''
df_seoul = df[df['시도명'] == '서울']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='서울 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
부산 등록대수
'''
df_seoul = df[df['시도명'] == '부산']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='부산 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
대구 등록대수
'''
df_seoul = df[df['시도명'] == '대구']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='대구 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
인천 등록대수
'''
df_seoul = df[df['시도명'] == '인천']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='인천 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
광주 등록대수
'''
df_seoul = df[df['시도명'] == '광주']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='광주 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
대전 등록대수
'''
df_seoul = df[df['시도명'] == '대전']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='대전 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
울산 등록대수
'''
df_seoul = df[df['시도명'] == '울산']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='울산 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
세종 등록대수
'''
df_seoul = df[df['시도명'] == '세종']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='세종 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
경기 등록대수
'''
df_seoul = df[df['시도명'] == '경기']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='경기 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
강원 등록대수
'''
df_seoul = df[df['시도명'] == '강원']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='강원 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
충북 등록대수
'''
df_seoul = df[df['시도명'] == '충북']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='충북 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
충남 등록대수
'''
df_seoul = df[df['시도명'] == '충남']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='충남 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
전북 등록대수
'''
df_seoul = df[df['시도명'] == '전북']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='전북 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
전남 등록대수
'''
df_seoul = df[df['시도명'] == '전남']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='전남 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
경북 등록대수
'''
df_seoul = df[df['시도명'] == '경북']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='경북 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
경남 등록대수
'''
df_seoul = df[df['시도명'] == '경남']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='경남 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


'''
제주 등록대수
'''
df_seoul = df[df['시도명'] == '제주']
df_seoul = df_seoul[df_seoul['시군구'] != '계']


fig = px.bar(
    df_seoul,
    x='시군구',
    y='계',
    title='제주 시군구별 자동차 등록대수',
    labels={'계': '등록대수', '시군구': '시군구'},
    color_discrete_sequence=['skyblue']
)

# X축 레이블 회전
fig.update_layout(
    xaxis=dict(tickangle=-45),
    title_x=0.5
)

fig.show()


