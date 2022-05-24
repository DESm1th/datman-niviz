import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import bids.config
from bids.layout import BIDSLayout, add_config_paths
from yamale.validators import DefaultValidators, Validator

from models import (tables, db, Entity, Component, Rating, TableColumn,
                    TableRow, Image)


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


def init_databases(config):
    """Creates all niviz-rater databases and tables.
    """
    connection = psycopg2.connect(config[SQLALCHEMY_DATABASE_URI])
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    for db_name in config['NIVIZ_SETTINGS']:
        try:
            cursor.execute(sql.SQL(f'CREATE DATABASE {db_name}'))
        except psycopg2.errors.DuplicateDatabase:
            continue

        engine = db.get_engine(bind=db_name)
        for table in tables:
            table.__table__.create(engine, checkfirst=True)


def build_index(db, bids_files, qc_spec):
    """
    Initialize database with objects
    """
    row_tpl = AxisNameTpl(Template(qc_spec['RowDescription']['name']),
                          qc_spec['RowDescription']['entities'])

    for c in qc_spec['Components']:
        component = ConfigComponent(**c)
        make_database(db,
                      component.build_qc_entities(bids_files),
                      component.available_ratings,
                      row_tpl)


def update_bids_configuration(bids_config):
    """
    Update configuration path for bids and return file paths
    for new configuration files
    """
    logging.debug(f'Replacing bids configuration with user={bids_config}')
    add_config_paths(user=bids_config)

    #### Default return value = ['./data/bids.json', '/mnt/tigrlab/archive/code/virtual_envs/lab_code_01/lib/python3.8/site-packages/bids/layout/config/bids.json', '/mnt/tigrlab/archive/code/virtual_envs/lab_code_01/lib/python3.8/site-packages/bids/layout/config/derivatives.json']
    return bids.config.get_option('config_paths').values()


def validate_config(
        qc_spec_config, bids_configs, schema_file='data/schema.yml'):
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


def populate_database(db, entities, available_ratings, row_tpl):
    # Step 0: We'll create our component and ratings
    component = Component()
    db.session.add(component)
    db.session.flush()
    for item in available_ratings:
        db.session.add(
            Rating(name=item, component_id=component.id)
        )

    # Step 1: Get set of row names to use and save in dictionary
    unique_rows = set([make_rowname(row_tpl, e.entities) for e in entities])
    for row in unique_rows:
        db.session.add(TableRow(name=row))

    unique_cols = set([e.column_name for e in entities])
    for col in unique_cols:
        db.session.add(TableColumn(name=col))

    db.session.flush()

    # Step 3: Create entities
    for e in entities:
        entity = Entity(name=e.name,
                        component_id=component.id,
                        rowname=make_rowname(row_tpl, e.entities),
                        columnname=e.column_name)
        db.session.add(entity)
        db.session.flush()

        for i in e.images:
            db.session.add(Image(path=i, entity_id=entity.id))

    db.session.commit()


def make_rowname(rowtpl, entities):
    keys = {k: v for k, v in entities.items() if k in rowtpl.entities}
    return rowtpl.tpl.substitute(keys)


def _is_subdict(big, small):
    return dict(big, **small) == big


def _get_key(bidsfile, entities):
    return tuple([bidsfile.entities[e] for e in entities])
