from playhouse.migrate import *


db = SqliteDatabase("db.sqlite")
migrator = SqliteMigrator(db)


# class Outcome(BaseModel):
#     doc_id = CharField(unique=True)


queued_field = BooleanField(default=False)

migrate(
    migrator.add_column('outcome', 'queued', queued_field),
)


# class Outcome(BaseModel):
#     doc_id = CharField(unique=True)
#     queued = BooleanField(default=False)
