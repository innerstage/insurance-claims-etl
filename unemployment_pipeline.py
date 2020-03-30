import pandas as pd
import xmltodict
import json
import os
import us
import csv

from bamboo_lib.helpers import grab_connector
from bamboo_lib.logger import logger
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep


class OpenStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Processing {}".format(params.get("state")))
        
        with open("unemployment_data/{}".format(params["state"]),"r") as file:
            xml = file.read()

        xml_dict = xmltodict.parse(xml)
        json_data = json.dumps(xml_dict)
        json_dict = json.loads(json_data)
        data = json_dict['r539cyState']['week']

        data_rows = {k:[] for k in data[0].keys()}

        for i in range(len(data)):
            for k in data[i].keys():
                data_rows[k].append(data[i][k])

        df = pd.DataFrame({k:data_rows[k] for k in data_rows.keys()})
        
        return df


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("Transforming {}".format(params.get("state")))
        df = prev
        
        column_names = {
            "stateName": "state_name",
            "weekEnded": "week_ended",
            "InitialClaims": "initial_claims",
            "ReflectingWeekEnded": "reflecting_week_end",
            "ContinuedClaims": "continued_claims",
            "CoveredEmployment": "covered_employment",
            "InsuredUnemploymentRate": "insured_unemployment_rate"
        }

        df = df.rename(columns=column_names)

        # US Mapping
        fips_map = us.states.mapping("name","fips")
        df["fips_code"] = "04000US" + df["state_name"].map(fips_map)

        df = df[["week_ended","reflecting_week_end","fips_code","state_name","initial_claims",
                 "continued_claims","covered_employment","insured_unemployment_rate"]]

        for c in ["week_ended", "reflecting_week_end"]:
            df[c] = pd.to_datetime(df[c], format="%m/%d/%Y")

        for c in ["initial_claims","continued_claims","covered_employment"]:
            df[c] = df[c].str.replace(",","").astype("int64")

        df["insured_unemployment_rate"] = df["insured_unemployment_rate"].astype("float64")

        # Append to CVS
        df.to_csv("output.csv", index=False, mode="a", quoting=csv.QUOTE_NONNUMERIC)

        return df


class UnemploymentPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(name="state", dtype=str),
            Parameter(name="db", dtype=str),
            Parameter(name="ingest", dtype=bool)
		]
    
    @staticmethod
    def steps(params):
        db_connector = grab_connector(__file__, params.get("db"))
        
        open_step = OpenStep()
        transform_step = TransformStep()
        load_step = LoadStep(
            table_name="unemployment_claims",
			connector=db_connector,
			if_exists="append",
			pk=["week_ended"]
        )

        steps = [open_step, transform_step, load_step] if params.get("ingest") else [open_step, transform_step]
        
        return steps


if __name__ == "__main__":
    filenames = sorted(list(os.listdir("unemployment_data")))
    for f in filenames:
        unemployment_pipeline = UnemploymentPipeline()
        unemployment_pipeline.run(
            {
                "state": f,
                "db": "postgres-local",
                "ingest": False
            }
        )