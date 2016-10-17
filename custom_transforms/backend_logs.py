""" Class to transform our native APIV2 logs to BigQuery.
"""
import json

import apache_beam as beam

from apache_beam.io.bigquery import BigQueryWrapper

from etl.lib.convert import ApiV2

from config import config

class FlattenLogs(beam.DoFn):
    """ Convert API > Pub/Sub > fluetnd > GCS logs to be BigQuery friendly.
    """
    def process(self, context):
        """ Flatten and return the log entry.

            * Assumes a "resource"; "relationships" not yet supported.
            * Adds pub/sub message_id and publish_time
        """
        converter = ApiV2()
        raw_message = json.loads(context.element)
        try:
            api_payload = raw_message["message"]["data"]["data"]["data"]
            message_id = raw_message["message"]["message_id"]
            publish_time = raw_message["message"]["publish_time"]
            resource_type = api_payload.get("type", "UnknownType")
            converter.create_resource(api_payload)
            response = dict(message_id=message_id, publish_time=publish_time)
            response.update(converter.resources[resource_type][0])
            # remove all marshmallow_missings:
            for key, value in response.iteritems():
                if value == '<marshmallow.missing>':
                    response[key] = None

            # nb: we singularize the entity for BQ table names
            yield resource_type.rstrip("s"), response
        except Exception as e:
            # print "SKIPPING(Exception = '{}'): poorly formatted event: '{}'".format(e, raw_message)
            yield "Error", "None"



class MultiTableWriteToBigQuery(beam.DoFn):
    """ Writes a multimap to BigQuery Tables.
    """
    def process(self, context, known_tables):
        """ Send input to appropriate tables in BigQuery.
        """
        cfg = config()
        bq = BigQueryWrapper()
        project_id = cfg.GCP_PROJECT
        dataset_id = cfg.DATASET_ID
        table_id, table_data = context.element

        if table_id in known_tables:
            status, errors = bq.insert_rows(project_id=project_id,
                                            dataset_id=dataset_id,
                                            table_id=table_id,
                                            rows=table_data)
            if not status:
                print "...table_id = {}".format(table_id)
                print "...tabledata={}".format(table_data)
                print "STATUS = {}".format(status)
                print "ERRORS = {}".format(errors)
        yield context.element
