"""
POC reading from GCS and writing to localhost.
"""
from __future__ import print_function
import argparse

import apache_beam as beam
from apache_beam import (coders, io, transforms)
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions

from custom_transforms import backend_logs

from config import config

from utils.bq_schemas import tables_in_dataset

def _process_args(argv):
    """ Parse command line args. Set some defaults.
    """
    cfg = config()
    project = "prod/*" if cfg == "realmassive-staging" else "staging/*"
    # input_file_pattern = "gs://logshipper-{}/*".format(project)
    input_file_pattern = "./api-logs-{}/*.gz".format(cfg.GCP_PROJECT)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default=input_file_pattern,
                        help="Input log files stored in GCS.")
    parser.add_argument('--output', dest='output', default="/tmp/flat-logs",
                        help="Output file to write data to.")
    known_args, pipeline_args = parser.parse_known_args(argv)
    return known_args, pipeline_args


def _pipeline_options(pipeline_args):
    """ Pipeline configuration.
    """
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    return pipeline_options


def run(argv=None):
    """ Runs the pipeline.

    Args:
        argv: Pipeline options as a list of arguments.
    """
    known_args, pipeline_args = _process_args(argv)
    # TODO: Setup execution in the cloud.
    pipeline_options = _pipeline_options(pipeline_args)
    p = beam.Pipeline(options=pipeline_options)

    cfg = config()
    dataset_path = "{}:{}".format(cfg.GCP_PROJECT, cfg.DATASET_ID)
    relevant_tables = [t.name for t in tables_in_dataset(dataset_path)]

    raw_logs = (
        p
        | io.Read(
              "ReadLogsFromGCS",
              beam.io.TextFileSource(
                  known_args.input,
                  coder=coders.BytesCoder()
              )
          )
        | beam.ParDo(
              "FlattenLogs",
              backend_logs.FlattenLogs()
          )
        | beam.GroupByKey("RecordsByKey")
        | beam.ParDo(
              "WriteToBigQuery",
              backend_logs.MultiTableWriteToBigQuery(),
              relevant_tables
          )
        # -------- OUTPUT OPTION #1 -- LocalHost; for debugging
        # writes to text files locally
        | io.Write(
              "WriteToLocalhost",
                  io.textio.WriteToText(
                  known_args.output,
                  file_name_suffix=".json"
              )
        )

    )
    p.run()


if __name__ == '__main__':
    run()
