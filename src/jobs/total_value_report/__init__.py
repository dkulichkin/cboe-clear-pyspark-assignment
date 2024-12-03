import os
import inspect
from pipetools import pipe

from jobs.total_value_report.extract import extract
from jobs.total_value_report.transform import transform
from jobs.total_value_report.load import load


def start(spark, **job_args):
    if "csv_path" in job_args:
        csv_path = job_args["csv_path"]
    else:
        root_dir = os.path.dirname(inspect.stack()[1][1])
        csv_path = os.path.join(root_dir, "../resources/trades.csv")

    # pylint:disable=E1131
    pipeline = pipe | extract | transform | load
    pipeline(spark, csv_path)
