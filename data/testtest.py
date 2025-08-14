import pandas as pd
import requests
import re
import time
import os

# CSV 경로
# file_path = "data/충청남도 천안시_불법주정차단속현황_2024_08.CSV"
file_path = "data/천안시_단속장소_위도경도.csv"
# CSV 읽기
df = pd.read_csv(file_path)

df.dropna(subset=['위도'], inplace=True)
print(df.head())
# csv로 저장
df.to_csv("data/testtest.csv", index=False, encoding="utf-8-sig")
