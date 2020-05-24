import pandas as pd

df = pd.read_csv("./unemployment_output/full_output.csv")

### CHANGE THE LAST WEEK OF INFORMATION HERE:
last_week = "2020-05-16"
#############################################

weeks = pd.date_range(end=last_week, periods=4, freq='7D')
weeks = [w.strftime("%Y-%m-%d") for w in weeks]

for (a,b) in [(weeks[0],weeks[1]), (weeks[1],weeks[2]), (weeks[2],weeks[3])]:
    val = df[(df["week_ended"] > a) & (df["week_ended"] <= b)]["initial_claims"].sum()
    print("SUM with {} < week_ended <= {}: {}".format(a,b,val))
