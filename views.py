import os
import logging

from flask import render_template, current_app, request
from flask_login import login_required

from . import niviz_bp
from .utils import set_db
from .models import Entity, Image, TableRow, TableColumn, Rating

logger = logging.getLogger(__name__)


@niviz_bp.route('/niviz-rater')
def index(study, pipeline):
    return render_template('niviz.html')


def _rating(rating):
    return {'id': rating.id, 'name': rating.name} if rating else None


def _fileserver(path, study, pipeline):
    """
    Transform local directory path to fileserver path
    """
    base = current_app.config['NIVIZ_SETTINGS'][f'{study}_{pipeline}']['base_path']
    img = os.path.relpath(path, base)
    return img


@niviz_bp.route('/api/overview')
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


@niviz_bp.route('/api/spreadsheet')
@set_db
def spreadsheet(study, pipeline):
    """
    Query database for information required to construct
    interactive table, yields for each TableRow it's
    set of entities
    """

    q = Entity.query.all()

    # Need to remove base path
    r = {
        "entities": [{
            "rowName":
            e.rowname,
            "columnName":
            e.columnname,
            "imagePaths":
            [_fileserver(i.path, study, pipeline) for i in e.images],
            "comment":
            e.comment,
            "failed":
            e.failed,
            "id":
            e.id,
            "name":
            e.name,
            "rating":
            _rating(e.rating)
        } for e in q]
    }
    return r


@niviz_bp.route('/api/entity/<int:entity_id>')
@set_db
def get_entity_info(study, pipeline, entity_id):
    try:
        e = Entity.query.get(entity_id)
    except IndexError:
        logger.error("Cannot find entity with specified ID!")
        return {}, 404

    r = {
        "name": e.name,
        "rating": _rating(e.rating),
        "comment": e.comment,
        "failed": e.failed,
        "imagePaths":
        [_fileserver(i.path, study, pipeline) for i in e.images],
        "id": e.id,
        "rowName": e.name,
        "columnName": e.name
    }
    return r


@niviz_bp.route('/api/entity/<int:entity_id>/view')
@set_db
def get_entity_view(study, pipeline, entity_id):
    """
    Retrieve full information for entity
    Yields:
        entity name
        array of entity image paths
        available ratings
        current rating for a given entity
    """

    entity = Entity.query.get(entity_id)

    q_rating = Rating.query.filter(Rating.component_id == entity.component_id)\
        .all()
    available_ratings = [_rating(r) for r in q_rating]

    response = {
        "entityId": entity.id,
        "entityName": entity.name,
        "entityRating": _rating(entity.rating),
        "entityComment": entity.comment,
        "entityAvailableRatings": available_ratings,
        "entityImages":
        [_fileserver(i.path, study, pipeline) for i in entity.images],
        "entityFailed": entity.failed
    }
    return response


@niviz_bp.route('/api/entity', methods=['POST'])
@set_db
def update_rating(study, pipeline):
    """
    Post body should contain information about:
        -   rating_id
        -   comment
        -   qc_rating
    """
    expected_keys = {'rating', 'comment', 'failed'}
    data = request.json
    if data is None:
        logger.info("No changes requested...")
        return
    logger.info("Received message from application!")
    update_keys = expected_keys.intersection(data.keys())
    logger.info("Updating keys")
    logger.info(update_keys)

    # Select entity
    entity = Entity.query.get(data['id'])
    logger.info(data)

    # Update entity with available keys
    for k in update_keys:
        setattr(entity, k, data[k])
    try:
        entity.save()
    except:
        return {}, 400

    return {}, 200


@niviz_bp.route('/api/export')
@set_db
def export_csv(study, pipeline):
    """
    Export participants.tsv CSV file
    """
    columns = TableColumn.query.order_by(TableColumn.name).all()
    row_items = TableRow.query.all()

    rows = [_make_row(r, columns) for r in row_items]
    header = [
        f"{c.name}\t{c.name}_passfail\t{c.name}_comment" for c in columns
    ]
    header = "\t".join(["subjects"] + header)
    csv = "\n".join([header] + rows)
    return csv


def _make_row(row, columns):
    """
    Given a set of Entities for a given row, create
    column entries
    """
    p = 0
    entities = row.entities
    entries = [row.name]
    empty = ("", "", "")
    for c in columns:
        try:
            e = entities[p]
            if e.columnname == c.name:
                entries.extend(e.entry)
                p += 1
            else:
                entries.extend(empty)
        except IndexError:
            entries.extend(empty)

    return "\t".join(entries)
