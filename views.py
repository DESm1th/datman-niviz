import os
import logging

from flask import (render_template, current_app, request, send_from_directory,
                   redirect, url_for, flash)
from flask_login import login_required, current_user

from . import niviz_bp
from niviz_rater.utils import set_db
from niviz_rater.models import Entity, Image, TableRow, TableColumn, Rating

logger = logging.getLogger(__name__)


@niviz_bp.before_request
def before_request():
    if 'study' not in request.view_args:
        flash("Malformed request")
        return redirect(url_for('main.index'))

    study = request.view_args['study']
    if not current_user.has_study_access(study):
        flash(f"Permission denied for study {study}")
        return redirect(url_for('main.index'))


@niviz_bp.route('/niviz-rater')
@login_required
def index(study, pipeline):
    return render_template('niviz.html')


@niviz_bp.route('/qc-img/<regex(".*"):image>')
@login_required
def serve_images(study, pipeline, image):
    base = current_app.config['NIVIZ_RATER_CONF'][
        f'{study}_{pipeline}']['base_dir']
    return send_from_directory(base, image)


def _rating(rating):
    return {'id': rating.id, 'name': rating.name} if rating else None


def _img_path(path, study, pipeline):
    """
    Transform local directory path to fileserver path
    """
    base = current_app.config['NIVIZ_RATER_CONF'][
        f'{study}_{pipeline}']['base_dir']
    img = os.path.relpath(path, base)
    return os.path.join("qc-img", img)


@niviz_bp.route('/api/overview')
@set_db
@login_required
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
@login_required
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
            [_img_path(i.path, study, pipeline) for i in e.images],
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
@login_required
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
        [_img_path(i.path, study, pipeline) for i in e.images],
        "id": e.id,
        "rowName": e.name,
        "columnName": e.name
    }
    return r


@niviz_bp.route('/api/entity/<int:entity_id>/view')
@set_db
@login_required
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
        [_img_path(i.path, study, pipeline) for i in entity.images],
        "entityFailed": entity.failed
    }
    return response


@niviz_bp.route('/api/entity', methods=['POST'])
@set_db
@login_required
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
        return
    update_keys = expected_keys.intersection(data.keys())

    entity = Entity.query.get(data['id'])

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
@login_required
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
