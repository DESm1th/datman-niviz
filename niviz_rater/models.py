from sqlalchemy.schema import UniqueConstraint

from dashboard.models import db, TableMixin


class Component(TableMixin, db.Model):
    '''
    Component component ID
    '''
    id = db.Column(db.Integer, primary_key=True)


class Rating(TableMixin, db.Model):
    '''
    Rating ID --> named rating mapping
    '''
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    component_id = db.Column(
        'component_id',
        db.Integer,
        db.ForeignKey('component.id'),
        nullable=False)


class TableColumn(TableMixin, db.Model):
    __tablename__ = 'tablecolumn'

    name = db.Column(db.String, primary_key=True)


class TableRow(TableMixin, db.Model):
    __tablename__ = 'tablerow'

    name = db.Column(db.String, primary_key=True)

    entities = db.relationship('Entity', order_by='Entity.columnname')


class Entity(TableMixin, db.Model):
    '''
    Single entity to QC
    '''
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    columnname = db.Column(
        'columnname_id',
        db.ForeignKey('tablecolumn.name'),
        nullable=False)
    rowname = db.Column(
        'rowname_id',
        db.ForeignKey('tablerow.name'),
        nullable=False)
    component_id = db.Column(
        'component_id',
        db.Integer,
        db.ForeignKey('component.id'),
        nullable=False)
    comment = db.Column(db.Text, default="")
    failed = db.Column(db.Boolean)
    rating_id = db.Column('rating_id', db.ForeignKey('rating.id'))

    rating = db.relationship('Rating', uselist=False)
    images = db.relationship('Image', back_populates='entity')

    @property
    def has_failed(self):
        if self.failed is True:
            return "Fail"
        elif self.failed is False:
            return "Pass"
        else:
            return ""

    @property
    def entry(self):
        if self.rating:
            rating = self.rating.name
        else:
            rating = ""
        return (
            rating,
            self.has_failed,
            self.comment or ""
        )


class Image(TableMixin, db.Model):
    '''
    Images used for an Entity to assess quality
    '''
    id = db.Column(db.Integer, primary_key=True)
    path = db.Column(db.Text)
    entity_id = db.Column(db.Integer, db.ForeignKey('entity.id'))

    entity = db.relationship('Entity', back_populates='images')

    __table_args__ = (UniqueConstraint(path), )


# This is defined to make it easier to dynamically change databases
# for the models at runtime
tables = [Component, Rating, TableColumn, TableRow, Entity, Image]
