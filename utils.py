import logging

from niviz_rater.models import tables

logger = logging.getLogger(__name__)


def set_db(func):
    """Set a database for the models at runtime.
    """
    def wrapper(study, pipeline, *args, **kwargs):
        db_name = f"{study}_{pipeline}"
        for table in tables:
            table.metadata.tables[table.__tablename__
                ].info['bind_key'] = db_name
        return func(study, pipeline, *args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper
