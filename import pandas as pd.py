import pandas as pd

# 파일 경로
file_path = "data/충청남도 천안시_불법주정차단속현황_20240812.CSV"

# 데이터 읽기
df = pd.read_csv(file_path, encoding="cp949")

# 날짜 컬럼에서 연도 추출
df['단속일자'] = pd.to_datetime(df['단속일자'], errors='coerce')
df['연도'] = df['단속일자'].dt.year

# 행정동별 집계 (연도별, 단속동별 건수)
grouped = df.groupby(['연도', '단속동']).size().reset_index(name='단속건수')

# 결측치 제거
grouped = grouped.dropna(subset=['단속동'])

import matplotlib.pyplot as plt

df.groupby(["연도", "단속동"]).size().unstack().T.plot(kind="bar", stacked=True, figsize=(12,6))
plt.ylabel("단속 건수")
plt.title("연도별 행정동 단속 건수")
plt.show()
