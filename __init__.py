from flask import Blueprint

niviz_bp = Blueprint(
    'niviz_rater',
    __name__,
    template_folder='templates',
    url_prefix='/study/<string:study_id>/pipeline/<string:pipeline>'
)
