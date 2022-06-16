import os
import logging
import yaml

from niviz_rater.models import tables

logger = logging.getLogger(__name__)

ENV_CONFIG_VAR = "NIVIZ_RATER_CONF"


def set_db(func):
    """Set a database for the models at runtime.
    """
    def wrapper(study, pipeline, *args, **kwargs):
        db_name = f"{study}_{pipeline}"
        for table in tables:
            table.metadata.tables[table.__tablename__].info[
                'bind_key'] = db_name
        return func(study, pipeline, *args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


def get_config():
    config_file = os.getenv(ENV_CONFIG_VAR)
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Config file {config_file} doesn't exist. Check the "
                     "value of the environment variable "
                     f"'{ENV_CONFIG_VAR}' before trying again.")
        return {}
    except TypeError:
        logger.error(f"Config file not provided. Please set {ENV_CONFIG_VAR}")
        return {}
    return config


def parse_db_name(db_name):
    """Parse the database name into the study and pipeline name fields.
    """
    fields = db_name.split("_")
    study = fields[0]
    pipeline = "_".join(fields[1:])
    return study, pipeline
