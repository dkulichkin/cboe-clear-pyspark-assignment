#!/usr/bin/python
import argparse
import importlib
import time
import os
import sys

if os.path.exists('libs.zip'):
    sys.path.insert(0, 'libs.zip')
else:
    sys.path.insert(0, './libs')

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

# pylint:disable=W0611
try:
    import pyspark
except:
    import findspark
    findspark.init()
finally:
    from pyspark.sql import SparkSession


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name', help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--job-args', nargs='*', help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")

    args = parser.parse_args()

    environment = {
        'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''
    }

    job_args = {}
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        job_args = {a[0]: a[1] for a in job_args_tuples}

    os.environ.update(environment)
    spark = SparkSession.builder.appName("CBoe Test").getOrCreate()
    job_module = importlib.import_module(f"jobs.{args.job_name}")

    start = time.time()
    job_module.start(spark, **job_args)
    end = time.time()

    print(f"Execution of job {args.job_name} took {end-start} seconds")
