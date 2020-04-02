import pandas as pd

df = pd.read_csv("./unemployment_output/full_output.csv")

for (a,b) in [("2020-03-07","2020-03-14"), ("2020-03-14","2020-03-21"), ("2020-03-21","2020-03-28")]:
    val = df[(df["week_ended"] > a) & (df["week_ended"] <= b)]["initial_claims"].sum()
    print("SUM with {} < week_ended <= {}: {}".format(a,b,val))
