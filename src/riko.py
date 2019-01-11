"""
Project Riko

Riko is a simple and light ORM for MySQL.
"""
import pymysql


class RikoConfig:
    """
    Define default database config here
    """
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'rinka',
        'password': 'rinka',
        'database': 'test',
        'autocommit': True
    }


class AbstractModel:
    """
    DO NOT inherit this, inherit `DictModel` or `ObjectModel` instead.
    """

    def __init__(self, db_config=None):
        """
        Create a Riko model object.
        :param db_config:
        """
        self.db_config_ = RikoConfig.db_config if db_config is None else db_config
        self.db_session_ = DBI.get_connection(self.db_config_)

    @classmethod
    def create(cls, _tx=None, **kwargs):
        pass

    def insert(self):
        pass

    def delete(self):
        pass

    def update(self):
        pass

    def insert_update(self):
        pass

    @classmethod
    def count(cls, _tx=None):
        pass

    @classmethod
    def has(cls, _tx=None, _where_clause=None, **_where_terms):

    @classmethod
    def select(cls, _tx=None, _columns=None):
        pass

    @classmethod
    def get(cls, _tx=None, _columns=None, _limit=None, _offset=None, _order=None, _where_clause=None, **_where_terms):
        pass

    @classmethod
    def get_one(cls, tx=None, _columns=None, **_where_terms):
        pass


class SqlQuery:
    def __init__(self):
        self._sql = None

    def __str__(self):
        return self._sql


class ConditionQuery(SqlQuery):
    def __init__(self, where=None):
        super().__init__()
        if where is not None:
            if type(where) in (list, tuple):
                self._where = where
            else:
                self._where = [str(where)]
        else:
            self._where = list()

    def where(self, condition_terms):
        if condition_terms is not None:
            if type(condition_terms) is (list, tuple):
                self._where.extend(condition_terms)
            else:
                self._where.append(str(condition_terms))

    def where_equal(self, **equal_condition_terms):
        if equal_condition_terms is not None:
            for (k, v) in equal_condition_terms.items():
                self._where.append("%s = %s" % (k, v))

class OrderQuery(ConditionQuery):
    def __init__(self, where=None, order_by=None):
        super().__init__(where)
        if order_by is not None:
            if type(order_by) is (list, tuple):
                self._order_by = order_by
            else:
                self._order_by = [str(order_by)]
        else:
            self._order_by = list()

    def order_by(self, column_terms):
        if column_terms is not None:
            if type(column_terms) is (list, tuple):
                self._where.extend(column_terms)
            else:
                self._where.append(str(column_terms))


class ReversibleQuery(OrderQuery):
    def __init__(self, where=None, order_by=None):
        super().__init__(where, order_by)
        self._warp_reverse = False

    def reverse(self):
        self._warp_reverse = not self._warp_reverse


class PaginationOrderQuery(ReversibleQuery):
    def __init__(self, where=None, limit=None, offset=None, order_by=None):
        super().__init__(where, order_by)
        self._limit = limit
        self._offset = offset

    def limit(self, limit):
        self._limit = limit

    def offset(self, offset):
        self._offset = offset








class SelectBuilder:


class DictModel(dict, AbstractModel):
    """
    Basic object model in dict structure, inherit this and set `pk` and `fields`.
    """

    # Primary key list
    pk = []

    # Fields list
    fields = []

    def __init__(self, db_config=None):
        """
        Create a new dict-like ORM object.
        :param db_config: db config using for connect to db, or None to use default `RikoConfig`
        """
        dict.__init__(self)
        AbstractModel.__init__(self, db_config)


class DBI:
    """
    DB engine interface.
    """

    @staticmethod
    def get_connection(db_config=None):
        return DBI(RikoConfig.db_config if db_config is None else db_config)

    def __init__(self, db_config):
        assert db_config is not None
        self._db_conf = db_config
        self._conn = pymysql.connect(**db_config)

    def close(self):
        self._conn.close()

    def query(self, sql, args, fetch_result=True):
        cursor = self._conn.cursor()
        cursor.execute(sql, args)
        if fetch_result:
            return cursor.fetchall()
        else:
            return cursor

    def with_transaction(self):
        _auto_commit = self._conn.get_autocommit()
        self._conn.autocommit(False)
        self._conn.begin()
        try:
            yield self._conn
            self._conn.commit()
        except Exception as ex:
            self._conn.rollback()
            raise ex
        finally:
            self._conn.autocommit(_auto_commit)
