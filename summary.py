import pandas as pd

df = pd.read_csv("./unemployment_output/full_output.csv")

### CHANGE THE LAST WEEK OF INFORMATION HERE:
last_week = "2020-07-11"
#############################################

weeks = pd.date_range(end=last_week, periods=3, freq='7D')
weeks = [w.strftime("%Y-%m-%d") for w in weeks]

val = []
for (a,b) in [(weeks[1],weeks[2]), (weeks[0],weeks[1])]:
    val.append(df[(df["week_ended"] > a) & (df["week_ended"] <= b)]["initial_claims"].sum())
    print("WEEK ENDED {}: {:,}".format(b, val[-1]))

print("CHANGE: {:,}".format(val[-2]-val[-1]))
