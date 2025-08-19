# 특정 키워드("앞", "부근", "주변")만 제거하고 나머지 텍스트는 유지하도록 전처리

import re
import pandas as pd

# 파일 경로 다시 지정
in_path = "data/충청남도 천안시_불법주정차단속현황_2022.CSV"
out_path = "data/충청남도_천안시_불법주정차단속현황_2022_clean_text_removed.csv"

# 1) 로드 (이미 enc 확인했으므로 cp949 우선)
for enc in ["utf-8", "cp949", "euc-kr"]:
    try:
        df = pd.read_csv(in_path, encoding=enc, low_memory=False)
        break
    except UnicodeDecodeError:
        continue

# 2) D열 컬럼명 (단속장소)
col_D = df.columns[3]

# 3) 문자열에서 '앞', '부근', '주변' 제거
df[col_D] = df[col_D].astype(str).str.replace(r"(앞|부근|주변)", "", regex=True)

# 4) 저장
df.to_csv(out_path, index=False, encoding="utf-8-sig")

out_path