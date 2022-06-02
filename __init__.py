import os

from flask import Blueprint

from niviz_rater.utils import get_config

import logging

logger = logging.getLogger(__name__)

niviz_bp = Blueprint(
    "niviz_rater",
    __name__,
    template_folder="templates",
    url_prefix="/study/<string:study>/pipeline/<string:pipeline>"
)


def register_bp(app):
    config = get_config()
    if not config:
        logger.error("Failed to register blueprint 'datman_niviz': "
                     "Configuration not found.")
        return
    app.register_blueprint(niviz_bp)
    app.config["NIVIZ_RATER_CONF"] = config

    base_uri = app.config["DATABASE_ROOT_URI"]
    for db_name in config:
        app.config["SQLALCHEMY_BINDS"][db_name] = f"{base_uri}/{db_name.lower()}"
    return app


from . import views
