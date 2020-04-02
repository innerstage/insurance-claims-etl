import pandas as pd
import csv
import us
import numpy as np

adv_df = pd.read_csv("./unemployment_data/advance_claims.tsv", sep="\t", header=0, skiprows=1)

adv_df = adv_df[:53]

adv_df[["Advance", "Prior Wk", "Change"]] = adv_df["Advance Prior Wk Change"].str.split(" ", n=3, expand=True)
adv_df[["Advance.1", "Prior Wk.1", "Change.1"]] = adv_df["Advance Prior Wk Change.1"].str.split(" ", n=3, expand=True)

df1 = pd.DataFrame({
    "week_ended": "2020-03-28",	
    "reflecting_week_end": "2020-03-21",	
    "fips_code": 0,
    "state_name": adv_df["STATE"].str.replace("*",""),
    "initial_claims": adv_df["Advance"],
    "continued_claims": adv_df["Advance.1"],
    "covered_employment": np.nan,
    "insured_unemployment_rate": np.nan
})

fips_map = us.states.mapping("name","fips")
df1["fips_code"] = "04000US" + df1["state_name"].map(fips_map)

print(df1.head())
print(df1.dtypes)
for c in ["initial_claims", "continued_claims"]:
    df1[c] = df1[c].str.replace(",","").astype(int)

df1["insured_unemployment_rate"] = df1["insured_unemployment_rate"].astype(float)


# Open partial Output and Concatenate
df2 = pd.read_csv("./unemployment_output/partial_output.csv")
print(df2.head())
print(df2.dtypes)

df = pd.concat([df1,df2])
df = df.sort_values(by=["reflecting_week_end","fips_code"], ascending=[False, True])
print(df.head())
print(df.dtypes)
df.to_csv("./unemployment_output/full_output.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)