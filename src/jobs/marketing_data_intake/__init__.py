import os
import inspect
from pipetools import pipe

from jobs.marketing_data_intake.extract import extract
from jobs.marketing_data_intake.transform import transform
from jobs.marketing_data_intake.load import load


def start(spark, **job_args):
    if "csv_path" in job_args:
        csv_path = job_args["csv_path"]
    else:
        root_dir = os.path.dirname(inspect.stack()[1][1])
        csv_path = os.path.join(root_dir, "../resources/bloomberg_raw.csv")

    # pylint:disable=E1131
    pipeline = pipe | extract | transform | load
    pipeline(spark, csv_path)
