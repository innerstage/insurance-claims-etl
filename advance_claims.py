import pandas as pd
import csv
import us
import numpy as np

### CHANGE THE LAST WEEK OF INFORMATION HERE:
last_week = "2020-08-15"
#############################################

weeks = pd.date_range(end=last_week, periods=3, freq='7D')
weeks = [w.strftime("%Y-%m-%d") for w in weeks]

def clean_advance_claims(filepath="./unemployment_data/advance_claims.tsv"):

    adv_df = pd.read_csv(filepath, sep="\t", header=0)

    df1 = pd.DataFrame({
        "week_ended": weeks[2],	
        "reflecting_week_end": weeks[1],	
        "fips_code": 0,
        "state_name": adv_df["State"],
        "initial_claims": adv_df["Advance"],
        "continued_claims": adv_df["Advance.1"],
        "covered_employment": np.nan,
        "insured_unemployment_rate": np.nan
    })

    df2 = pd.DataFrame({
        "week_ended": weeks[1],	
        "reflecting_week_end": weeks[0],
        "fips_code": 0,
        "state_name": adv_df["State"],
        "initial_claims": adv_df["Prior Wk"],
        "continued_claims": adv_df["Prior Wk.1"],
        "covered_employment": np.nan,
        "insured_unemployment_rate": np.nan
    })

    df1 = pd.concat([df1,df2])

    fips_map = us.states.mapping("name","fips")
    df1["fips_code"] = "04000US" + df1["state_name"].map(fips_map)

    for c in ["initial_claims", "continued_claims"]:
        try:
            df1[c] = df1[c].str.replace(",","").astype(int)
        except:
            pass

    df1["insured_unemployment_rate"] = df1["insured_unemployment_rate"].astype(float)

    print(df1.head())
    print(df1.dtypes)

    return df1


def concatenate_and_save(df1):
    # Opens partial Output and Concatenate
    df2 = pd.read_csv("./unemployment_output/partial_output.csv")
    df2 = df2[df2["week_ended"] < weeks[1]]
    print(df2.head())
    print(df2.dtypes)

    df = pd.concat([df1,df2])
    df = df.sort_values(by=["reflecting_week_end","fips_code"], ascending=[False, True])
    print(df.head())
    print(df.dtypes)
    df.to_csv("./unemployment_output/full_output.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)


if __name__ == "__main__":
    df1 = clean_advance_claims()
    concatenate_and_save(df1)
    print("Completed!")