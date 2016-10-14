"""
POC reading from GCS and writing to localhost.
"""
import argparse
# import json
# import logging
import zlib

import google.cloud.dataflow as df
from google.cloud.dataflow import (coders, io, transforms)
from google.cloud.dataflow.utils.options import PipelineOptions
from google.cloud.dataflow.utils.options import SetupOptions

def _process_args(argv):
    """ Parse command line args. Set some defaults.
    """
    # project = "staging/*"
    # input_file_pattern = "gs://logshipper-{}".format(project)
    input_file_pattern = "./api-logs/*"
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default=input_file_pattern,
                        help="Input log files stored in GCS.")
    parser.add_argument('--output', dest='output', default="/tmp/no-clue",
                        help="Output file to write data to.")
    known_args, pipeline_args = parser.parse_known_args(argv)
    return known_args, pipeline_args


def run(argv=None):
    """ Runs the pipeline.

    Args:
        argv: Pipeline options as a list of arguments.
    """
    known_args, pipeline_args = _process_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = df.Pipeline(options=pipeline_options)

    raw_logs = (p
                | io.Read("ReadFromGCS", df.io.TextFileSource(
                          known_args.input,  # gs://my-bucket/files-*
                          compression_type="gzip",
                          coder=coders.BytesCoder()))
                # | transforms.Map(lambda x: [zlib.decompress(x)])
                | io.Write("WriteToLocalhost", df.io.TextFileSink(
                           known_args.output,
                           coder=coders.StrUtf8Coder())))  # local file
    p.run()

    print raw_logs

if __name__ == '__main__':
    run()
