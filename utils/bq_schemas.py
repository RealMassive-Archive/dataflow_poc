""" A few utils for reading/creating schemas in BigQuery.
"""
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from etl.lib.convert import csv_schema

from config import config

GCP_PROJECT = config().GCP_PROJECT
DATASET_ID = "data_pipeline"

def tables_in_dataset(dataset):
    """ Returns the list of tables in a dataset.
    """
    if type(dataset) == str:
        project, dataset = dataset.split(":")
        client = bigquery.Client(project=project)
        dataset = client.dataset("{}".format(dataset))

    tables, page_token = dataset.list_tables()
    all_tables = tables
    while page_token is not None:
        tables, page_token = dataset.list_tables(page_token=page_token)
        all_tables.extend(tables)
    map(lambda x: x.reload(), all_tables)  # reload all the metadata
    return all_tables

def get_schemas(project=GCP_PROJECT, dataset=DATASET_ID, tables=[]):
    client = bigquery.Client(project=project)
    dataset = client.dataset("{}".format(dataset))
    if not dataset.exists():
        return
    all_tables = tables_in_dataset(dataset)
    for table in all_tables:
        print table.name, table.schema
    return all_tables

def make_tables(project=GCP_PROJECT, dataset=DATASET_ID):
    """ makes all schemas...
    """
    client = bigquery.Client(project=project)
    dataset = client.dataset("{}".format(dataset))
    if not dataset.exists():
        print "Dataset doesn't exist."
        return
    for table_name, table_schema in csv_schema.iteritems():
        bq_schema = [
            SchemaField(name=_["name"], field_type=_["type"], mode="NULLABLE") \
                for _ in table_schema
        ]
        bq_schema.extend([
            SchemaField(name="message_id", field_type="STRING", mode="REQUIRED"),
            SchemaField(name="publish_time", field_type="TIMESTAMP", mode="REQUIRED")
        ])
        table = dataset.table(table_name, bq_schema)
        table.create()


def delete_tables_in_dataset(project=GCP_PROJECT, dataset=DATASET_ID):
    """ Careful, son...
    """
    client = bigquery.Client(project=project)
    dataset = client.dataset("{}".format(dataset))
    if not dataset.exists():
        print "Dataset doesn't exist."
        return

    go_on = raw_input(
        "DELETE ALL TABLES IN {}:{}? (y/n) ".format(
            project,
            dataset.name)
    ).lower() == 'y'

    if not go_on:
        return

    all_tables = tables_in_dataset(dataset)
    for table in all_tables:
        table.delete()
        print "Deleted table: {}:{}.{}".format(
            project, dataset.name, table.table_id)
