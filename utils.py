from .models import tables


def set_db(func):
    """Set a database for the models at runtime.
    """
    def wrapper(study, pipeline):
        db_name = f"{study}_{pipeline}"
        for table in tables:
            table.metadata.tables[table.__tablename__
                ].info['bind_key'] = db_name
        return func(study, pipeline)
    return wrapper
