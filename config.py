import os

class ProdConfig:
    GCP_PROJECT = "realmassive-prod"
    DATASET_ID = "data_pipeline"


class StagingConfig:
    GCP_PROJECT = "realmassive-staging"
    DATASET_ID = "data_pipeline"


config_map = {
    "staging": StagingConfig(),
    "prod": ProdConfig()
}

def config():
    project = os.environ.get("GCP_PROJECT", "staging")
    return config_map[project]
