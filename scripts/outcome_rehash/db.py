from peewee import *

db = SqliteDatabase("db.sqlite")


class BaseModel(Model):
    class Meta:
        database = db


class Outcome(BaseModel):
    id = CharField(unique=True)
