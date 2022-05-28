import os

from flask import Blueprint

from .utils import get_config

niviz_bp = Blueprint(
    'niviz_rater',
    __name__,
    template_folder='templates',
    url_prefix='/study/<string:study>/pipeline/<string:pipeline>'
)


def register_bp(app):
    config = get_config()
    if not config:
        logger.error("Failed to register blueprint 'datman_niviz': "
                     "Configuration not found.")
        return
    app.register_blueprint(niviz_bp)
    app.config['NIVIZ_RATER_CONF'] = config
    return app


from . import views
