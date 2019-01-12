"""
Project Riko

Riko is a simple and light ORM for MySQL.
"""
import contextlib
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

    @staticmethod
    def set_default(db_config):
        RikoConfig.db_config = db_config

    @staticmethod
    def update_default(**db_config):
        RikoConfig.db_config.update(db_config)


class AbstractModel:
    """
    DO NOT inherit this, inherit `DictModel` or `ObjectModel` instead.
    """
    __metaclass__ = ABCMeta
    _abstract_inner_var = {"db_config_", "db_session_"}

    def __init__(self, _db_config=None):
        """
        Create a Riko model object.
        :param _db_config: database to mapping
        """
        self.db_config_ = RikoConfig.db_config if _db_config is None else _db_config
        self.db_session_ = DBI.get_connection(self.db_config_)

    @classmethod
    def deserialize(cls, db_conf, **terms):
        try:
            des_obj = cls(db_conf)
        except:
            des_obj = cls()
        if terms is not None:
            for (k, v) in terms.items():
                des_obj._set_value(k, v)
        return des_obj

    def columns(self):
        _columns = self._get_columns()
        for k in _columns:
            yield k

    @abstractmethod
    def _get_ak(self): pass

    @abstractmethod
    def _set_ak(self, value): pass

    @abstractmethod
    def _get_pk(self): pass

    @abstractmethod
    def _get_fields(self): pass

    @abstractmethod
    def _get_columns(self): pass

    @abstractmethod
    def _get_value(self, column): pass

    @abstractmethod
    def _set_value(self, column, value): pass

    @classmethod
    def create(cls, _db_config=None, **kwargs):
        try:
            created = cls(_db_config)
        except:
            created = cls()
        if kwargs is not None:
            for (k, v) in kwargs.items():
                created._set_value(k, v)
        return created

    def insert(self):
        insert_dict = dict()
        if type(self) is dict:
            insert_dict = self
        else:
            for k in self.columns():
                actual_value = self._get_value(k)
                if actual_value is not None:
                    insert_dict[k] = actual_value
        auto_key = self._get_ak()
        re_affect_id = (InsertQuery(self.__class__)
                        .set_session(self.db_session_)
                        .values(**insert_dict)
                        .go(return_last_id=True if auto_key is not None else False))
        if auto_key is not None:
            self._set_ak(re_affect_id)
        return re_affect_id

    def delete(self):
        delete_dict = dict()
        pks = self._get_pk()
        for k in pks:
            actual_value = self._get_value(k)
            if actual_value is not None:
                delete_dict[k] = actual_value
        return (DeleteQuery(self.__class__)
                .set_session(self.db_session_)
                .where(**delete_dict)
                .go())

    def save(self):
        update_pk_dict = dict()
        pks = self._get_pk()
        for k in pks:
            actual_value = self._get_value(k)
            if actual_value is not None:
                update_pk_dict[k] = actual_value
        update_field_dict = dict()
        # TODO primary key may be update but cannot handle now
        for k in self.columns():
            update_field_dict[k] = self._get_value(k)
        return (UpdateQuery(self.__class__)
                .set_session(self.db_session_)
                .set(**update_field_dict)
                .where(**update_pk_dict)
                .go())

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
        return SelectQuery(cls, _columns).set_session(_tx)

    @classmethod
    def get(cls, _tx=None, _columns=None, _limit=None, _offset=None,
            _order=None, _where_raw=None, _args=None, **_where_terms):
        return (SelectQuery(cls, columns=_columns, limit=_limit, offset=_offset, order_by=_order)
                .set_session(_tx)
                .where_raw(_where_raw)
                .where(**_where_terms)
                .get(_args, parse_model=True))

    @classmethod
    def get_one(cls, _tx=None, _columns=None, _where_raw=None, _args=None, **_where_terms):
        return cls.get(_tx=_tx, _columns=_columns, _limit=1, _offset=None,
                       _order=None, _where_raw=_where_raw, _args=_args, **_where_terms)


class SqlQuery:
    __metaclass__ = ABCMeta

    _KW_TABLE = "{{__RIKO_TABLE__}}"
    _KW_INSERT_REPLACE = "{{__RIKO_INSERT_REPLACE__}}"
    _KW_FIELDS = "{{__RIKO_FIELDS__}}"
    _KW_VALUES = "{{__RIKO_VALUES__}}"
    _KW_ON_DUPLICATE_KEY_UPDATE = "{{__RIKO_DUPLICATE_KEY__}}"
    _KW_WHERE = "{{__RIKO_WHERE__}}"
    _KW_DISTINCT = "{{__RIKO_DISTINCT__}}"
    _KW_GROUP_BY = "{{__RIKO_GROUP_BY__}}"
    _KW_HAVING = "{{__RIKO_HAVING__}}"
    _KW_ORDER_BY = "{{__RIKO_ORDER_BY__}}"
    _KW_LIMIT = "{{__RIKO_LIMIT__}}"
    _KW_OFFSET = "{{__RIKO_OFFSET__}}"

    _Insert_Template = """
{{__RIKO_INSERT_REPLACE__}} INTO {{__RIKO_TABLE__}}({{__RIKO_FIELDS__}})
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
{{__RIKO_LIMIT__}}
{{__RIKO_OFFSET__}}
"""

    def __init__(self, clazz):
        assert clazz is not None
        self._sql = None
        self._dbi = None
        self._clz_meta = clazz
        self._temporary_dbi = False
        self._args = dict()

    def __str__(self):
        return self._sql

    def set_session(self, dbi=None):
        if dbi is None:
            self._dbi = DBI(RikoConfig.db_config)
            self._temporary_dbi = True
        else:
            self._dbi = dbi
        return self

    def get(self, args=None, parse_model=False):
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        try:
            raw_result = self._dbi.query(self._sql, self._args)
        finally:
            if self._temporary_dbi:
                self._dbi.close()
        return [self._clz_meta.deserialize(self._dbi.get_config(), **kvt) for kvt in raw_result] \
            if parse_model else raw_result

    def only(self, args=None):
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        try:
            ret = self._dbi.query(self._sql, self._args)
            return ret[0] if ret and len(ret) > 0 else None
        finally:
            if self._temporary_dbi:
                self._dbi.close()

    def go(self, args=None, return_last_id=False):
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        try:
            return self._dbi.query(self._sql, self._args,
                                   return_pattern=DBI.RETURN_AFFECTED_ROW
                                   if return_last_id is False else DBI.RETURN_LAST_ROW_ID)
        finally:
            if self._temporary_dbi:
                self._dbi.close()

    def cursor(self, args=None):
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        return self._dbi.query(self._sql, self._args, return_pattern=DBI.RETURN_NONE)

    def with_cursor(self, args=None):
        ptr = None
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        try:
            ptr = self._dbi.query(self._sql, self._args, return_pattern=DBI.RETURN_NONE)
            yield ptr
        finally:
            if ptr:
                ptr.close()
            if self._temporary_dbi:
                self._dbi.close()

    @abstractmethod
    def _prepare_sql(self): pass


class ConditionQuery(SqlQuery):
    def __init__(self, clazz, where=None):
        super().__init__(clazz)
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


class OrderedQuery(ConditionQuery):
    def __init__(self, clazz, where=None, order_by=None):
        super().__init__(clazz, where)
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
                self._order_by.extend(column_terms)
            else:
                self._order_by.append(str(column_terms))
        return self

    def _construct_order_by_clause(self):
        if len(self._order_by) == 0:
            return ""
        return "ORDER BY " + ", ".join(self._order_by)

    @abstractmethod
    def _prepare_sql(self): pass


class ReversibleQuery(OrderedQuery):
    def __init__(self, clazz, where=None, order_by=None):
        super().__init__(clazz, where, order_by)
        self._warp_reverse = False

    def reverse(self):
        self._warp_reverse = not self._warp_reverse
        return self

    @abstractmethod
    def _prepare_sql(self): pass


class PaginationOrderQuery(ReversibleQuery):
    def __init__(self, clazz, where=None, limit=None, offset=None, order_by=None):
        super().__init__(clazz, where, order_by)
        self._limit = limit
        self._offset = offset

    def limit(self, limit):
        self._limit = limit
        return self

    def offset(self, offset):
        self._offset = offset
        return self

    def _construct_limit_clause(self):
        if self._limit is None:
            return ""
        return "LIMIT " + str(self._limit)

    def _construct_offset_clause(self):
        if self._offset is None:
            return ""
        return "OFFSET " + str(self._offset)

    @abstractmethod
    def _prepare_sql(self): pass


class InsertQuery(SqlQuery):
    def __init__(self, clazz):
        super().__init__(clazz)
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
        self._sql = SqlQuery._Insert_Template
        r_dict = {
            SqlQuery._KW_INSERT_REPLACE: self._construct_insert_operator_clause(),
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_FIELDS: self._construct_insert_fields_clause(),
            SqlQuery._KW_VALUES: self._construct_insert_values_clause(),
            SqlQuery._KW_ON_DUPLICATE_KEY_UPDATE: self._construct_on_duplicate_key_update_clause()
        }
        self._sql = SqlRender.render(self._sql, r_dict)


class DeleteQuery(ConditionQuery):
    def __init__(self, clazz, where=None):
        super().__init__(clazz, where)

    def _prepare_sql(self):
        self._sql = SqlQuery._Delete_Template
        r_dict = {
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_WHERE: self._construct_where_clause()
        }
        self._sql = SqlRender.render(self._sql, r_dict)


class UpdateQuery(ConditionQuery):
    def __init__(self, clazz, where=None):
        super().__init__(clazz, where)
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
        self._sql = SqlQuery._Update_Template
        r_dict = {
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_WHERE: self._construct_where_clause(),
            SqlQuery._KW_FIELDS: self._construct_update_set_clause()
        }
        self._sql = SqlRender.render(self._sql, r_dict)


class SelectQuery(PaginationOrderQuery):
    def __init__(self, clazz, columns=None, where=None, limit=None, offset=None, order_by=None):
        super().__init__(clazz, where, limit, offset, order_by)
        self._return_columns = list() if columns is None else list(columns)
        self._distinct = False
        self._group_by = list()
        self._having = list()

    def distinct(self):
        self._distinct = True
        return self

    def group_by(self, group_terms):
        if group_terms is not None:
            if type(group_terms) in (list, tuple):
                self._group_by.extend(group_terms)
            else:
                self._group_by.append(str(group_terms))
        return self

    def having_raw(self, having_terms):
        if having_terms is not None:
            if type(having_terms) in (list, tuple):
                self._having.extend(having_terms)
            else:
                self._having.append(str(having_terms))
        return self

    def having(self, **having_terms):
        if having_terms is not None:
            for (k, v) in having_terms.items():
                self._having.append(k + " = %(__RIKO_HAVING_" + k + ")s")
                self._args["__RIKO_HAVING_" + k] = v
        return self

    def _construct_distinct_clause(self):
        return "DISTINCT" if self._distinct else ""

    def _construct_select_fields_clause(self):
        if len(self._return_columns) == 0:
            return "*"
        return ",".join(self._return_columns)

    def _construct_group_by_clause(self):
        if len(self._group_by) == 0:
            return ""
        return "GROUP BY " + ",".join(self._group_by)

    def _construct_having_clause(self):
        if len(self._group_by) == 0 or len(self._having) == 0:
            return ""
        return "HAVING " + " AND ".join(self._having)

    def _prepare_sql(self):
        self._sql = SqlQuery._Select_Template
        r_dict = {
            SqlQuery._KW_DISTINCT: self._construct_distinct_clause(),
            SqlQuery._KW_FIELDS: self._construct_select_fields_clause(),
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_WHERE: self._construct_where_clause(),
            SqlQuery._KW_GROUP_BY: self._construct_group_by_clause(),
            SqlQuery._KW_HAVING: self._construct_having_clause(),
            SqlQuery._KW_ORDER_BY: self._construct_order_by_clause(),
            SqlQuery._KW_LIMIT: self._construct_limit_clause(),
            SqlQuery._KW_OFFSET: self._construct_offset_clause(),
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
        for (k, v) in args.items():
            _render = _render.replace(k, v)
        return _render


class DictModel(AbstractModel, dict):
    """
    Basic object model in dict structure, inherit this and set `pk` and `fields`.
    """
    # Auto increment key
    ak = None

    # Primary key list
    pk = []

    # Fields list
    fields = []

    def __init__(self, _db_config=None):
        """
        Create a new dict-like ORM object.
        :param _db_config: db config using for connect to db, or None to use default `RikoConfig`
        """
        dict.__init__(self)
        AbstractModel.__init__(self, _db_config)

    def _get_ak(self):
        return self.ak

    def _set_ak(self, value):
        self[self.ak] = value

    def _get_pk(self):
        return self.pk

    def _get_fields(self):
        return self.fields

    def _get_columns(self):
        return self.pk + self.fields

    def _get_value(self, column):
        return self[column] if column in self else None

    def _set_value(self, column, value):
        if column in self._get_columns():
            self[column] = value
        else:
            raise Exception("Miss match column in Model: " + column)


class ObjectModel(AbstractModel):
    """
    Basic object model in object mapping structure, inherit this and set `pk` and `fields`.
    """

    # Auto increment key
    ak = None

    # Primary key list
    pk = []

    def __init__(self, _db_config=None):
        super().__init__(_db_config)
        self._model_fields = None
        self._model_columns = None

    def _get_ak(self):
        return self.ak

    def _set_ak(self, value):
        if hasattr(self, self.ak):
            setattr(self, self.ak, value)
        else:
            raise Exception("Miss match auto increment column in Model: " + self.ak)

    def _get_pk(self):
        return self.pk

    def _get_fields(self):
        if self._model_fields is None:
            self._model_fields = list(vars(self).keys())
            for _ik in self._abstract_inner_var:
                self._model_fields.remove(_ik)
            self._model_fields.remove("_model_fields")
            self._model_fields.remove("_model_columns")
            for pkt in self.pk:
                self._model_fields.remove(pkt)
        return self._model_fields

    def _get_columns(self):
        return self._get_fields() + self.pk

    def _get_value(self, column):
        if hasattr(self, column):
            return getattr(self, column)
        else:
            return None

    def _set_value(self, column, value):
        if hasattr(self, column):
            setattr(self, column, value)
        else:
            raise Exception("Miss match column in Model: " + column)


class DBI:
    """
    DB engine interface.
    """
    RETURN_NONE = 0
    RETURN_CURSOR = 1
    RETURN_RESULT = 2
    RETURN_LAST_ROW_ID = 3
    RETURN_AFFECTED_ROW = 4

    @staticmethod
    def get_connection(db_config=None):
        return DBI(RikoConfig.db_config if db_config is None else db_config)

    def __init__(self, db_config):
        assert db_config is not None
        self._db_conf = db_config
        self._conn = pymysql.connect(**db_config)

    def get_config(self):
        return self._db_conf

    def close(self):
        self._conn.close()

    def query(self, sql, args, return_pattern=RETURN_RESULT):
        cursor = self._conn.cursor()
        affected = cursor.execute(sql, args)
        if return_pattern == DBI.RETURN_RESULT:
            fetched = cursor.fetchall()
            names = [cd[0] for cd in cursor.description]
            return [dict(zip(names, v)) for v in fetched]
        elif return_pattern == DBI.RETURN_CURSOR:
            return cursor
        elif return_pattern == DBI.RETURN_LAST_ROW_ID:
            return cursor.lastrowid
        elif return_pattern == DBI.RETURN_AFFECTED_ROW:
            return affected
        else:
            return

    @contextlib.contextmanager
    def transaction(self):
        _auto_commit = self._conn.get_autocommit()
        self._conn.autocommit(False)
        self._conn.begin()
        try:
            yield self
            self._conn.commit()
        except Exception as ex:
            self._conn.rollback()
            raise ex
        finally:
            self._conn.autocommit(_auto_commit)
