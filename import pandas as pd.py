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

import caas_jupyter_tools
caas_jupyter_tools.display_dataframe_to_user(name="연도별 행정동별 단속 건수 집계표", dataframe=grouped)
