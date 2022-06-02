#!/usr/bin/env python
"""Initialize configured niviz-rater databases.

This script will read from the environment variable NIVIZ_RATER_CONF to
find the configured databases and input files for niviz-rater.

Usage:
    init_db.py [<db_name>]

Arguments:
    <db_name>       The name of a database from the config file to initialize.
                    If absent, all databases in the file will be initialized.

"""
import os
import yaml
import json
import logging.config
from string import Template
from collections import namedtuple
from itertools import groupby
from dataclasses import dataclass

from docopt import docopt
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy.exc import IntegrityError
import bids.config
from bids.layout import BIDSLayout, add_config_paths
import yamale
from yamale.validators import DefaultValidators, Validator

import dashboard
from dashboard import create_app, db
import niviz_rater.models as models
from niviz_rater.utils import get_config, set_db

logger = logging.getLogger(__name__)

base_dir = os.path.dirname(os.path.realpath(__file__))
DEFAULT_BIDS = os.path.join(base_dir, "../data/bids.json")
DEFAULT_SCHEMA = os.path.join(base_dir, "../data/schema.yaml")

AxisNameTpl = namedtuple('AxisNameTpl', ('tpl', 'entities'))


class Entities(Validator):
    """
    Class to enable validation of BIDS entities
    from pyBIDS JSON configuration files

    Note:
        This class cannot be used in isolation. Instead
        create a copy of the class with the `valid_configs`
        property set
    """

    __slots__ = 'valid_configs'

    tag = 'Entities'

    def __init__(self, *args, **kwargs):
        """
        Perform a check to ensure that `valid_configs` is
        defined

        Raises:
            AttributeError: If `valid_configs` is not defined
        """

        if self.valid_configs is None:
            raise AttributeError('`valid_configs` property of Entities '
                                 'class is not set. Create copy of class '
                                 'and set `valid_configs`!')
        super(Entities, self).__init__(*args, **kwargs)

    def _is_valid(self, value):
        return value in _get_valid_entities(self.valid_configs)


class ConfigComponent:
    """
    Configurable Factory class for building QC components
    from list of images
    """
    def __init__(self, entities, name, column, images, ratings):
        self.entities = entities
        self.name = name
        self.column = column
        self.image_descriptors = images
        self.available_ratings = ratings

    def _group_by_entities(self, bidsfiles):
        """
        Sort list of bidsfiles by requested entities
        """
        filtered = [
            b for b in bidsfiles if all(k in b.entities for k in self.entities)
        ]
        return groupby(sorted(filtered,
                              key=lambda x: _get_key(x, self.entities)),
                       key=lambda x: _get_key(x, self.entities))

    def find_matches(self, images, image_descriptor):

        matches = [
            b for b in images if _is_subdict(b.entities, image_descriptor)
        ]
        if len(matches) > 1:
            logger.error(f"Got {len(matches)} matches to entity,"
                         " expected 1!")
            logger.error(f"Matching specification:\n {image_descriptor}")
            print_matches = "\n".join([m.path for m in matches])
            raise ValueError

        try:
            return matches[0]
        except IndexError:
            logger.error(f"Found 0 matches for\n {image_descriptor}!")
            return

    def build_qc_entities(self, image_list):
        """
        Build QC Entities given a list of images

        Arguments:
            image_list          List of BIDSFile images
                                to build QC entities from
        """

        qc_entities = []

        for key, group in self._group_by_entities(image_list):
            group_entities = list(group)
            matched_images = [
                self.find_matches(group_entities, i)
                for i in self.image_descriptors
            ]

            # Remove missing data
            matched_images = [m for m in matched_images if m is not None]
            if matched_images:
                qc_entities.append(
                    QCEntity(images=[m.path for m in matched_images],
                             entities={
                                 k: matched_images[0].entities[k]
                                 for k in self.entities
                             },
                             tpl_name=self.name,
                             tpl_column_name=self.column))

        return qc_entities


@dataclass
class QCEntity:
    """
    Helper class to represent a single QC entity
    """
    images: list
    entities: dict
    tpl_name: str
    tpl_column_name: str

    @property
    def name(self):
        return Template(self.tpl_name).substitute(self.entities)

    @property
    def column_name(self):
        return Template(self.tpl_column_name).substitute(self.entities)


def main():
    arguments = docopt(__doc__)
    db_name = arguments['<db_name>']

    config = get_config()

    app = create_app()
    context = app.app_context()
    context.push()

    databases = [db_name] if db_name else config.keys()
    for db_name in databases:
        initialize_db(db_name, config[db_name], app.config)


def load_json(file):
    with open(file, 'r') as f:
        result = json.load(f)
    return result


def initialize_db(db_name, config, dash_config):
    make_database(db_name, dash_config)
    make_tables(db_name)

    qc_spec = get_qc_spec(db_name, config)
    bids_files = get_files(db_name, config, qc_spec)
    study, pipeline = db_name.split("_")
    build_index(study, pipeline, bids_files, qc_spec)
    add_pipeline(db_name)


def make_database(db_name, dash_config):
    connection = psycopg2.connect(dash_config['SQLALCHEMY_DATABASE_URI'])
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    try:
        cursor.execute(sql.SQL(f'CREATE DATABASE {db_name}'))
    except psycopg2.errors.DuplicateDatabase:
        pass


def make_tables(db_name):
    engine = db.get_engine(bind=db_name)
    for table in models.tables:
        table.__table__.create(engine, checkfirst=True)


def get_qc_spec(db_name, config):
    bids_configs = update_bids_configuration(
        bids_config=config.get("bids_config", DEFAULT_BIDS))

    try:
        spec_file = config["qc_spec"]
    except KeyError:
        logger.error(f"{db_name} missing required setting 'qc_spec'. "
                     "Aborting database initialization.")
        return
    return validate_config(spec_file, bids_configs,
            schema_file=config.get("schema", DEFAULT_SCHEMA))


def get_files(db_name, config, qc_spec):
    try:
        base_dir = config["base_dir"]
    except KeyError:
        logger.error(f"{db_name} missing required setting 'base_dir'. "
                     "Aborting database initialization.")
        return
    return get_qc_bidsfiles(base_dir, qc_spec)


def update_bids_configuration(bids_config=DEFAULT_BIDS):
    """
    Update configuration path for bids and return file paths
    for new configuration files
    """
    logger.debug(f'Replacing bids configuration with user={bids_config}')
    add_config_paths(user=bids_config)
    return bids.config.get_option('config_paths').values()


def validate_config(qc_spec_config, bids_configs, schema_file=DEFAULT_SCHEMA):
    """
    Validate a YAML-based configuration file against a
    schema file containing BIDS entity constraints

    Args:
        qc_spec_config (str): Path to YAML configuration file to be validated
        bids_configs (:obj: `list` of :obj: `str): List of paths to pyBIDS
            configuration files to include in validation
        schema_file (str): Path to YAML schema to validate against. Defaults
            to niviz_rater/data/schema.yaml

    Returns:
        config (dict): Parsed configuration file

    Raises:
        YamaleError: If validation fails due to invalid `config` file
    """
    ConfiguredEntities = _configure_entity_validator(bids_configs)
    validators = DefaultValidators.copy()
    validators[ConfiguredEntities.tag] = ConfiguredEntities

    schema = yamale.make_schema(schema_file, validators=validators)
    yamaledata = yamale.make_data(qc_spec_config)
    yamale.validate(schema, yamaledata)

    with open(qc_spec_config, 'r') as f:
        config = yaml.load(f, Loader=yaml.CLoader)

    return config


def _configure_entity_validator(bids_configs):
    """
    A little hack to work around Yamale requiring statically
    defined classes for validation.

    Dynamically the set of BIDS JSON configuration files
    to validate against
    """
    ConfiguredEntities = Entities
    ConfiguredEntities.valid_configs = bids_configs
    return ConfiguredEntities


def _get_valid_entities(bids_config_files):
    valid_configs = [
        load_json(file)['entities'] for file in bids_config_files
    ]
    enumstrs = []
    for configfile in valid_configs:
        enumstrs.extend([entity['name'] for entity in configfile])
    return enumstrs


def get_qc_bidsfiles(qc_dataset, qc_spec):
    """
    Get BIDSFiles associated with qc_dataset
    """
    layout = BIDSLayout(qc_dataset,
                        validate=False,
                        index_metadata=False,
                        config=["user"])
    bidsfiles = layout.get(extension=qc_spec['ImageExtensions'])
    return bidsfiles


@set_db
def build_index(study, pipeline, bids_files, qc_spec):
    """
    Initialize database with objects
    """
    row_tpl = AxisNameTpl(Template(qc_spec['RowDescription']['name']),
                          qc_spec['RowDescription']['entities'])

    for c in qc_spec['Components']:
        component = ConfigComponent(**c)
        add_records(component.build_qc_entities(bids_files),
                    component.available_ratings,
                    row_tpl)


def add_records(entities, available_ratings, row_tpl):
    component = add_component()
    add_ratings(available_ratings, component)
    add_rownames(entities, row_tpl)
    add_colnames(entities)

    for item in entities:
        entity = add_entity(item, component, row_tpl)
        add_images(item, entity)


def add_component():
    component = models.Component()
    db.session.add(component)
    db.session.commit()
    return component


def add_ratings(available_ratings, component):
    for item in available_ratings:
        db.session.add(models.Rating(name=item, component_id=component.id))
        db.session.commit()


def add_rownames(entities, row_tpl):
    unique_rows = set([make_rowname(row_tpl, e.entities) for e in entities])
    for row in unique_rows:
        db.session.add(models.TableRow(name=row))
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()


def add_colnames(entities):
    unique_cols = set([e.column_name for e in entities])
    for col in unique_cols:
        db.session.add(models.TableColumn(name=col))
    db.session.commit()


def make_rowname(rowtpl, entities):
    keys = {k: v for k, v in entities.items() if k in rowtpl.entities}
    return rowtpl.tpl.substitute(keys)


def add_entity(e, component, row_tpl):
    entity = models.Entity(name=e.name,
                           component_id=component.id,
                           rowname=make_rowname(row_tpl, e.entities),
                           columnname=e.column_name)
    db.session.add(entity)
    db.session.commit()
    return entity


def add_images(e, entity):
    for i in e.images:
        db.session.add(models.Image(path=i, entity_id=entity.id))
    db.session.commit()


def _is_subdict(big, small):
    return dict(big, **small) == big


def _get_key(bidsfile, entities):
    return tuple([bidsfile.entities[e] for e in entities])


def add_pipeline(db_name):
    study_id, key = db_name.split("_")
    study = dashboard.models.Study.query.get(study_id)
    if not study:
        logger.error(f"Study {study_id} doesnt exist. Dashboard will "
                     f"ignore niviz-rater database {db_name}")
        return
    study.add_pipeline(key, 'niviz_rater.index', 'Niviz Rater QC', 'study')


if __name__ == "__main__":
    main()
