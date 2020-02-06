from peewee import *

db = SqliteDatabase("db.sqlite")


class BaseModel(Model):
    class Meta:
        database = db


class Outcome(BaseModel):
    doc_id = CharField(unique=True)
    queued = BooleanField(default=False)
