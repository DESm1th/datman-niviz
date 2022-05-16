import logging

from flask import render_template
from flask_login import login_required

from . import niviz_bp
from .utils import set_db
from .models import Entity, Image, TableRow, TableColumn, Rating

logger = logging.getLogger(__name__)


@niviz_bp.route('/')
@set_db
def summary(study, pipeline):
    """
    Pull summary information from index, yield:
        - rows of entity index
        - qc items per row entity of index
        - remaining un-rated images
        - total number of ratings required
    """
    n_unrated = Entity.query.filter(Entity.failed == None).count()
    n_rows = TableRow.query.count()
    n_cols = TableColumn.query.count()
    n_entities = Entity.query.count()

    return {
        "numberOfUnrated": n_unrated,
        "numberOfRows": n_rows,
        "numberOfColumns": n_cols,
        "numberOfEntities": n_entities
    }
