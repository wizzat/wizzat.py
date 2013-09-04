from .pghelper import execute, sql_where_from_params

class DBTable(object):
    """
    This is a micro-ORM for the purposes of not having dependencies on Django or SQLAlchemy.
    """

    def __init__(self, **kwargs):
        for field in self.fields:
            setattr(self, field, kwargs.get(field, None))

    def get_dict(self):
        return { field : getattr(self, field) for field in self.fields }

    @classmethod
    def find_by(self, **kwargs):
        sql = """
            SELECT *
            FROM {table_name}
            where {where_clause}
        """.format(
            table_name = self.table_name,
            where_clause = sql_where_from_params(**kwargs)
        )

        for row in iter_results(self.conn, sql, **kwargs):
            yield RepoFile(**row)

    def lock_for_processing(self, nowait = False):
        nowait = "nowait" if nowait else ""
        sql = "select * from {table_name} where {key_field} = %({key_field}s) for update {nowait}".format(
            table_name = self.table_name,
            key_field  = self.key_field,
            nowait     = nowait,
        )

        execute(self.conn, sql, **self.get_dict())

    def insert(self):
        kv = { x:y for x,y in self.get_dict() if y }
        fields = kv.keys()
        values = [ kv[x] for x in fields ]
        sql = "insert into {table_name} ({fields}) values ({values})".format(
            table_name = self.table_name,
            fields = ', '.join(fields),
            values = ', '.join([ "%({})s" for x in fields ]),
        )

        execute(self.conn, sql, **kv)

    def update(self):
        sql = "update {table_name} set {field_equality}".format(
            table_name = self.table_name,
            field_equality = ', '.join([ "{0} = %({0})s".format(x) for x in self.fields ])
        )

        execute(self.conn, sql, **self.get_dict())
