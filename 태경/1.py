# 전국 공영주차장 개수와 등록차량대수 비교
import numpy as np
import pandas as pd

df_car = pd.read_csv('전국_시군구_차량등록대수.csv', encoding='cp949')
df_car.drop(columns='Unnamed: 8', inplace=True)
df_car.info()
df_car['시도명'].unique()
df_car['시군구'].unique()
df_car['계']

df_parking = pd.read_csv('전국_시군구_공영주차장.csv')
df_parking
df_parking.info()
df_parking['CTPRVN_NM'].unique()

df_car.groupby('시도명')['계'].sum().dropna()
x1 = df_car.groupby('시군구')['계'].sum().dropna()

df_parking.groupby('CTPRVN_NM')['PARKNG_SPCE_CO'].sum().dropna()
y1 = df_parking.groupby('SIGNGU_NM')['PARKNG_SPCE_CO'].sum().dropna()

df1 = pd.DataFrame({
    '차량 수': x1,
    '주차 가능 대수': y1
}).reset_index()
df1
df1 = df1.rename(columns={'index': '지역구'})

df1['공영주차장 보급률'] = df1['주차 가능 대수'] / df1['차량 수'] * 100
df1
df1['지역구'].unique()
df1[df1['지역구'] == '천안시 동남구']
df1[df1['지역구'] == '천안시 서북구']
df1.loc[df1['공영주차장 보급률'].idxmin()]
df1.loc[df1['공영주차장 보급률'].idxmax()]
df1.sort_values('공영주차장 보급률')
df1['공영주차장 보급률'].describe()