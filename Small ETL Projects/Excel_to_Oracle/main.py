import pandas as pd
import funtions as f

df = pd.read_csv(r"D:\DADE\EXCEL_TO_ORACLE\powerlifting_dataset.csv")

clean_data = f.cleaning(df)
f.connecting(clean_data)

