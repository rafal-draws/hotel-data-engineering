import os
import pandas as pd

cwd = os.path.dirname(os.path.realpath(__file__))
df = pd.read_excel(cwd+"\\HotelFinalDataset.xlsx")
df.to_csv(cwd+"\\HotelFinalDataset.csv", sep = ",", encoding="utf-8")
