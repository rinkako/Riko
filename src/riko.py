"""
Project Riko

Riko is a simple and light ORM for MySQL.
"""
from abc import ABCMeta, abstractmethod
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
    __metaclass__ = ABCMeta

    def __init__(self, db_config=None):
        """
        Create a Riko model object.
        :param db_config:
        """
        self.db_config_ = RikoConfig.db_config if db_config is None else db_config
        self.db_session_ = DBI.get_connection(self.db_config_)

    @abstractmethod
    def get_pk(self): pass

    @abstractmethod
    def get_fields(self): pass

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
        pass

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
    __metaclass__ = ABCMeta

    _KW_TABLE = "{{__RIKO_TABLE__}}"
    _KW_INSERT_REPLACE = "{{__RIKO_INSERT_REPLACE__}}"
    _KW_IGNORE = "{{__RIKO_IGNORE__}}"
    _KW_FIELDS = "{{__RIKO_FIELDS__}}"
    _KW_VALUES = "{{__RIKO_VALUES__}}"
    _KW_ON_DUPLICATE_KEY_UPDATE = "{{__RIKO_DUPLICATE_KEY__}}"
    _KW_WHERE = "{{__RIKO_WHERE__}}"
    _KW_DISTINCT = "{{__RIKO_DISTINCT__}}"
    _KW_GROUP_BY = "{{__RIKO_GROUP_BY__}}"
    _KW_HAVING = "{{__RIKO_RIKO_HAVING__}}"
    _KW_ORDER_BY = "{{__RIKO_ORDER_BY__}}"

    _Insert_Template = """
{{__RIKO_INSERT_REPLACE__}} {{__RIKO_IGNORE__}} INTO {{__RIKO_TABLE__}}({{__RIKO_FIELDS__}})
VALUES ({{__RIKO_VALUES__}})
{{__RIKO_DUPLICATE_KEY__}}
"""
    _Delete_Template = """
DELETE FROM {{__RIKO_TABLE__}}
{{__RIKO_WHERE__}}
"""

    _Update_Template = """
UPDATE {{__RIKO_TABLE__}}
SET {{__RIKO_FIELDS__}}
{{__RIKO_WHERE__}}
"""
    _Select_Template = """
SELECT {{__RIKO_DISTINCT__}} {{__RIKO_FIELDS__}}
FROM {{__RIKO_TABLE__}}
{{__RIKO_WHERE__}}
{{__RIKO_GROUP_BY__}}
{{__RIKO_HAVING__}}
{{__RIKO_ORDER_BY__}}
"""

    def __init__(self):
        self._sql = None
        self._dbi = None
        self._clz_meta = None
        self._args = dict()

    def __str__(self):
        return self._sql

    def binding(self, clz, dbi):
        self._dbi = dbi
        self._clz_meta = clz

    def get(self, args=None):
        self._prepare_sql()
        return self._dbi.query(self._sql, args)

    def only(self, args=None):
        self._prepare_sql()
        ret = self._dbi.query(self._sql, args)
        return ret[0] if ret and len(ret) > 0 else None

    def go(self, args=None):
        self._prepare_sql()
        return self._dbi.query(self._sql, args, affected_row=True)

    def cursor(self, args=None):
        self._prepare_sql()
        return self._dbi.query(self._sql, args, fetch_result=False)

    def with_cursor(self, args=None):
        ptr = None
        try:
            self._prepare_sql()
            ptr = self._dbi.query(self._sql, args, fetch_result=False)
            yield ptr
        finally:
            if ptr:
                ptr.close()

    @abstractmethod
    def _prepare_sql(self): pass


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

    def where_raw(self, condition_terms):
        if condition_terms is not None:
            if type(condition_terms) in (list, tuple):
                self._where.extend(condition_terms)
            else:
                self._where.append(str(condition_terms))
        return self

    def where(self, **equal_condition_terms):
        if equal_condition_terms is not None:
            for (k, v) in equal_condition_terms.items():
                self._where.append(k + " = %(__RIKO_WHERE_" + k + ")s")
                self._args["__RIKO_WHERE_" + k] = v
        return self

    def _construct_where_clause(self):
        if len(self._where) == 0:
            return ""
        return "WHERE " + " AND ".join(self._where)

    @abstractmethod
    def _prepare_sql(self): pass


class OrderQuery(ConditionQuery):
    def __init__(self, where=None, order_by=None):
        super().__init__(where)
        if order_by is not None:
            if type(order_by) in (list, tuple):
                self._order_by = order_by
            else:
                self._order_by = [str(order_by)]
        else:
            self._order_by = list()

    def order_by(self, column_terms):
        if column_terms is not None:
            if type(column_terms) in (list, tuple):
                self._where.extend(column_terms)
            else:
                self._where.append(str(column_terms))
        return self

    @abstractmethod
    def _prepare_sql(self): pass


class ReversibleQuery(OrderQuery):
    def __init__(self, where=None, order_by=None):
        super().__init__(where, order_by)
        self._warp_reverse = False

    def reverse(self):
        self._warp_reverse = not self._warp_reverse
        return self

    @abstractmethod
    def _prepare_sql(self): pass


class PaginationOrderQuery(ReversibleQuery):
    def __init__(self, where=None, limit=None, offset=None, order_by=None):
        super().__init__(where, order_by)
        self._limit = limit
        self._offset = offset

    def limit(self, limit):
        self._limit = limit
        return self

    def offset(self, offset):
        self._offset = offset
        return self

    @abstractmethod
    def _prepare_sql(self): pass


class InsertQuery(SqlQuery):
    def __init__(self):
        super().__init__()
        self._on_duplicate_key_update = False
        self._on_duplicate_key_ignore = False
        self._on_duplicate_key_replace = False
        self._insert_fields = list()
        self._insert_values = list()
        self._duplicate_update = list()

    def values_raw(self, insert_field_terms, insert_value_terms):
        if insert_value_terms is None or insert_value_terms is None:
            return self
        assert len(insert_field_terms) == len(insert_value_terms)
        if insert_field_terms is not None:
            if type(insert_field_terms) in (list, tuple):
                self._insert_fields.extend(insert_field_terms)
                self._insert_values.extend(insert_value_terms)
            else:
                self._insert_fields.append(str(insert_field_terms))
                self._insert_values.append(str(insert_value_terms))
        return self

    def values(self, **insert_terms):
        if insert_terms is not None:
            for (k, v) in insert_terms.items():
                self._insert_fields.append(k)
                self._insert_values.append("%(__RIKO_VALUES_" + k + ")s")
                self._args["__RIKO_VALUES_" + k] = v
        return self

    def on_duplicate_key_update_raw(self, update_terms):
        if update_terms is not None:
            if type(update_terms) in (list, tuple):
                self._duplicate_update.extend(update_terms)
            else:
                self._duplicate_update.append(str(update_terms))
        return self

    def on_duplicate_key_update(self, **update_terms):
        self._on_duplicate_key_update = True
        if update_terms is not None:
            for (k, v) in update_terms.items():
                self._duplicate_update.append(k + " = %(__RIKO_UPSERT_" + k + ")s")
                self._args["__RIKO_UPSERT_" + k] = v
        return self

    def ignore(self):
        self._on_duplicate_key_ignore = True
        return self

    def replace(self):
        self._on_duplicate_key_replace = True
        return self

    def _construct_insert_operator_clause(self):
        if self._on_duplicate_key_replace is True:
            return "REPLACE"
        elif self._on_duplicate_key_ignore is True:
            return "INSERT IGNORE"
        else:
            return "INSERT"

    def _construct_insert_fields_clause(self):
        assert len(self._insert_fields) > 0
        return ", ".join(self._insert_fields)

    def _construct_insert_values_clause(self):
        assert len(self._insert_values) > 0
        return ", ".join(self._insert_values)

    def _construct_on_duplicate_key_update_clause(self):
        if self._on_duplicate_key_update is False:
            return ""
        assert len(self._duplicate_update) > 0
        return "ON DUPLICATE KEY UPDATE " + ", ".join(self._duplicate_update)

    def _prepare_sql(self):
        self._sql = SqlQuery._Delete_Template
        r_dict = {
            SqlQuery._KW_INSERT_REPLACE: self._construct_insert_operator_clause(),
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_FIELDS: self._construct_insert_fields_clause(),
            SqlQuery._KW_VALUES: self._construct_insert_values_clause(),
            SqlQuery._KW_ON_DUPLICATE_KEY_UPDATE: self._construct_on_duplicate_key_update_clause()
        }
        self._sql = SqlRender.render(self._sql, r_dict)


class DeleteQuery(ConditionQuery):
    def __init__(self, where=None):
        super().__init__(where)

    def _prepare_sql(self):
        self._sql = SqlQuery._Delete_Template
        r_dict = {
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_WHERE: self._construct_where_clause()
        }
        self._sql = SqlRender.render(self._sql, r_dict)


class UpdateQuery(ConditionQuery):
    def __init__(self, where=None):
        super().__init__(where)
        self._update_set = list()

    def set_raw(self, update_terms):
        if update_terms is not None:
            if type(update_terms) in (list, tuple):
                self._update_set.extend(update_terms)
            else:
                self._update_set.append(str(update_terms))
        return self

    def set(self, **update_terms):
        if update_terms is not None:
            for (k, v) in update_terms.items():
                self._update_set.append(k + " = %(__RIKO_SET_" + k + ")s")
                self._args["__RIKO_SET_" + k] = v
        return self

    def _construct_update_set_clause(self):
        assert len(self._update_set) > 0
        return ", ".join(self._update_set)

    def _prepare_sql(self):
        self._sql = SqlQuery._Delete_Template
        r_dict = {
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_WHERE: self._construct_where_clause(),
            SqlQuery._KW_FIELDS: self._construct_update_set_clause()
        }
        self._sql = SqlRender.render(self._sql, r_dict)


class SqlRender:
    @staticmethod
    def render(template, args):
        """
        Render sql naively by replace.
        :param template: string template in `SqlQuery`
        :param args: a dict for render
        :return: rendered sql string
        """
        _render = template
        for (k, v) in args:
            _render = _render.replace(k, v)
        return _render


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

    def get_pk(self):
        return self.pk

    def get_fields(self):
        return self.fields


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

    def query(self, sql, args, fetch_result=True, affected_row=False):
        cursor = self._conn.cursor()
        affected = cursor.execute(sql, args)
        if fetch_result:
            return cursor.fetchall()
        elif affected_row is False:
            return cursor
        else:
            return affected

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
