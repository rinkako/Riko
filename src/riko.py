"""
Project Riko

Riko is a simple and light ORM for MySQL.
DB Engine default to be pymysql, since not thread safe.
"""
import contextlib
from abc import ABCMeta, abstractmethod
import pymysql


class INSERT:
    DUPLICATE_KEY_EXCEPTION = 0
    DUPLICATE_KEY_REPLACE = 1
    DUPLICATE_KEY_IGNORE = 2
    DUPLICATE_KEY_UPDATE = 3


class JOIN:
    NATURAL_JOIN = 0
    INNER_JOIN = 1
    LEFT_JOIN = 2
    RIGHT_JOIN = 3


class Riko:
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
        """
        Set default db config for Riko.
        :param db_config: a dict for pymysql connection
        """
        Riko.db_config = db_config

    @staticmethod
    def update_default(**db_config):
        """
        Update some terms in db connection config.
        :param db_config: terms for update connection config dict
        """
        Riko.db_config.update(db_config)


class AbstractModel(metaclass=ABCMeta):
    __metaclass__ = ABCMeta

    """
    Abstract ORM model.
    DO NOT inherit this, inherit `DictModel` or `ObjectModel` instead.
    """
    _abstract_inner_var = {"db_config_", "dbi"}

    # Config
    _DB_CONF = None

    # Auto increment key
    ak = None

    # Primary key list
    pk = ()

    def __init__(self, _db_config=None):
        """
        Create a Riko model object.
        :param _db_config: database to mapping
        """
        if _db_config is None:
            _db_config = self._DB_CONF
        self.db_config_ = Riko.db_config if _db_config is None else _db_config
        self.dbi = DBI.get_connection(self.db_config_)

    @classmethod
    def deserialize(cls, db_conf, **terms):
        """
        Parse a dict-like object to `cls` object.
        For internal use, do not use it in your application.
        :param db_conf: db connection config
        :param terms: dict for parsing to the model object
        :return: parsed object in `cls` type
        """
        try:
            des_obj = cls(db_conf)
        except:
            des_obj = cls()
        if terms is not None:
            for (k, v) in terms.items():
                des_obj.set_value(k, v)
        return des_obj

    def columns(self):
        """
        Get a iterator for columns in this model.
        """
        _columns = self.get_columns()
        for k in _columns:
            yield k

    @classmethod
    def get_ak_name(cls):
        """
        Get auto increment key name of this model
        """
        return cls.ak

    @classmethod
    def get_pk_name(cls):
        """
        Get a list of primary key name of this model
        """
        return cls.pk

    @abstractmethod
    def get_ak(self):
        """
        Get value of auto increment id field
        """
        pass

    @abstractmethod
    def set_ak(self, value):
        """
        Set value of auto increment id field
        :param value: value to be set
        """
        pass

    def get_pk(self):
        """
        Get value of primary keys into a dict
        """
        pk_dict = dict()
        pks = self.get_pk_name()
        for k in pks:
            actual_value = self.get_value(k)
            if actual_value is not None:
                pk_dict[k] = actual_value
        return pk_dict

    @abstractmethod
    def get_fields(self):
        """
        Get fields name list, without primary keys
        """
        pass

    @abstractmethod
    def get_columns(self):
        """
        Get all fields name list, with primary keys
        """
        pass

    @abstractmethod
    def get_value(self, column):
        """
        Get value of a specific field.
        :param column: field name
        :return: field value
        """
        pass

    @abstractmethod
    def set_value(self, column, value):
        """
        Set value for a specific field
        :param column: field name
        :param value: value to be set
        """
        pass

    @classmethod
    def new(cls, _db_config=None, **kwargs):
        """
        Create a new ORM object, but not save to DB. Alias for `create`.
        :param _db_config: db connection config, None to use default
        :param kwargs: terms for init object
        :return: created ORM object
        """
        return cls.create(_db_config=_db_config, **kwargs)

    @classmethod
    def create(cls, _db_config=None, **kwargs):
        """
        Create a new ORM object, but not save to DB.
        :param _db_config: db connection config, None to use default
        :param kwargs: terms for init object
        :return: created ORM object
        """
        try:
            created = cls(_db_config)
        except:
            created = cls()
        if kwargs is not None:
            for (k, v) in kwargs.items():
                created.set_value(k, v)
        return created

    def insert(self, t=None, on_duplicate_key_replace=INSERT.DUPLICATE_KEY_EXCEPTION, **duplicate_key_update_term):
        """
        Insert this object into DB.
        :param t transaction connection object
        :param on_duplicate_key_replace: operation when primary key duplicated
        :param duplicate_key_update_term: terms for `ON DUPLICATE KEY UPDATE`
        :return: if `ak` is declared, return inserted auto increment id, otherwise return affected row count
        """
        insert_dict = dict()
        if type(self) is dict:
            insert_dict = self
        else:
            for k in self.columns():
                actual_value = self.get_value(k)
                if actual_value is not None:
                    insert_dict[k] = actual_value
        auto_key = self.get_ak_name()
        is_replace = False
        is_ignore = False
        if on_duplicate_key_replace == INSERT.DUPLICATE_KEY_REPLACE:
            is_replace = True
            duplicate_key_update_term = {}
        elif on_duplicate_key_replace == INSERT.DUPLICATE_KEY_IGNORE:
            is_ignore = True
            duplicate_key_update_term = {}
        elif on_duplicate_key_replace == INSERT.DUPLICATE_KEY_EXCEPTION:
            duplicate_key_update_term = {}
        re_affect_id = (SingleInsertQuery(self.__class__)
                        .set_session(model_db_conf=self._DB_CONF, dbi=t if t is not None else self.dbi)
                        .ignore(is_ignore)
                        .replace(is_replace)
                        .on_duplicate_key_update(**duplicate_key_update_term)
                        .values(**insert_dict)
                        .go(return_last_id=True if auto_key is not None else False))
        if auto_key is not None and self.get_ak() is None:
            self.set_ak(re_affect_id)
        return re_affect_id

    def delete(self, t=None):
        """
        Delete this object from DB.
        :param t transaction connection object
        :return: affected row count
        """
        return (DeleteQuery(self.__class__)
                .set_session(model_db_conf=self._DB_CONF, dbi=t if t is not None else self.dbi)
                .where(**self.get_pk())
                .go())

    def update(self, t=None):
        """
        Flush the change of this object to DB. Alias for `save`.
        :param t transaction connection object
        :return: affected row count
        """
        return self.save(t=t)

    def save(self, t=None):
        """
        Flush the change of this object to DB.
        :param t transaction connection object
        :return: affected row count
        """
        update_field_dict = dict()
        # TODO primary key may be update but cannot handle now
        for k in self.columns():
            update_field_dict[k] = self.get_value(k)
        return (UpdateQuery(self.__class__)
                .set_session(model_db_conf=self._DB_CONF, dbi=t if t is not None else self.dbi)
                .set(**update_field_dict)
                .where(**self.get_pk())
                .go())

    @classmethod
    def count(cls, t=None, _where_raw=None, _args=None, **_where_terms):
        """
        Count object satisfied given conditions.
        :param t: connection context, None to use default
        :param _where_raw: where condition tuple, each element give a condition and combined with `AND`
        :param _args: argument dict for SQL rendering
        :param _where_terms: where condition terms, only equal condition support only, combined with `AND`
        :return: a number of count result
        """
        cnt_ret = cls.get(t=t, return_columns=("count(1)",), _where_raw=_where_raw,
                          _args=_args, _parse_model=False, **_where_terms)
        if len(cnt_ret) > 0:
            return cnt_ret[0]["count(1)"]
        else:
            return 0

    @classmethod
    def has(cls, t=None, _where_raw=None, _args=None, **_where_terms):
        """
        Find is there any object satisfied given conditions.
        :param t: connection context, None to use default
        :param _where_raw: where condition tuple, each element give a condition and combined with `AND`
        :param _args: argument dict for SQL rendering
        :param _where_terms: where condition terms, only equal condition support only, combined with `AND`
        :return: a boolean of existence find result
        """
        return cls.count(t=t, _where_raw=_where_raw, _args=_args, **_where_terms) > 0

    @classmethod
    def select(cls, t=None, return_columns=None):
        """
        Begin a select query.
        :param t: connection context, None to use default
        :param return_columns: return columns tuple, None to return all fields in mapping table
        """
        return SelectQuery(cls, return_columns).set_session(model_db_conf=cls._DB_CONF, dbi=t)

    @classmethod
    def select_query(cls, t=None, return_columns=None):
        """
        Begin a select query.
        :param t: connection context, None to use default
        :param return_columns: return columns tuple, None to return all fields in mapping table
        """
        return cls.select(t=t, return_columns=return_columns)

    @classmethod
    def delete_query(cls, t=None):
        """
        Begin a delete query.
        :param t: connection context, None to use default
        """
        return DeleteQuery(cls).set_session(model_db_conf=cls._DB_CONF, dbi=t)

    @classmethod
    def update_query(cls, t=None):
        """
        Begin a update query.
        :param t: connection context, None to use default
        """
        return UpdateQuery(cls).set_session(model_db_conf=cls._DB_CONF, dbi=t)

    @classmethod
    def insert_query(cls, t=None):
        """
        Begin a insert query.
        :param t: connection context, None to use default
        """
        return SingleInsertQuery(cls).set_session(model_db_conf=cls._DB_CONF, dbi=t)

    @classmethod
    def insert_many(cls, t=None):
        """
        Begin a batch insert query.
        :param t: connection context, None to use default
        """
        return BatchInsertQuery(cls).set_session(model_db_conf=cls._DB_CONF, dbi=t)

    @classmethod
    def get_many(cls, t=None, return_columns=None, _where_raw=None, _limit=None, _offset=None,
                 _order=None, _args=None, _parse_model=True, for_update=False, **_where_terms):
        """
        Get objects satisfied given conditions. Alias for `get`.
        :param t: connection context, None to use default
        :param return_columns: return columns tuple, None to return all fields in mapping table
        :param _where_raw: where condition tuple, each element give a condition and combined with `AND`
        :param _limit: limit of query result row number
        :param _offset: offset of query result
        :param _order: ordering fields name tuple
        :param _args: argument dict for SQL rendering
        :param _parse_model: True to parse result to a list of ORM model objects, False to get list of dict objects
        :param for_update: Is select for update
        :param _where_terms: where condition terms, only equal condition support only, combined with `AND`
        :return: query result in the form of `_parse_model` pattern, default by a list of ORM models
        """
        return cls.get(t=t, return_columns=return_columns, _where_raw=_where_raw, _limit=_limit, _offset=_offset,
                       _order=_order, _args=_args, _parse_model=_parse_model, for_update=for_update, **_where_terms)

    @classmethod
    def get(cls, t=None, return_columns=None, _where_raw=None, _limit=None, _offset=None,
            _order=None, _args=None, _parse_model=True, for_update=False, **_where_terms):
        """
        Get objects satisfied given conditions.
        :param t: connection context, None to use default
        :param return_columns: return columns tuple, None to return all fields in mapping table
        :param _where_raw: where condition tuple, each element give a condition and combined with `AND`
        :param _limit: limit of query result row number
        :param _offset: offset of query result
        :param _order: ordering fields name tuple
        :param _args: argument dict for SQL rendering
        :param _parse_model: True to parse result to a list of ORM model objects, False to get list of dict objects
        :param for_update: Is select for update
        :param _where_terms: where condition te rms, only equal condition support only, combined with `AND`
        :return: query result in the form of `_parse_model` pattern, default by a list of ORM models
        """
        return (SelectQuery(cls, columns=return_columns, limit=_limit, offset=_offset, order_by=_order)
                .set_session(model_db_conf=cls._DB_CONF, dbi=t)
                .where_raw(*_where_raw if _where_raw else [])
                .where(**_where_terms)
                .for_update(for_update)
                .get(_args, parse_model=_parse_model))

    @classmethod
    def get_one(cls, t=None, return_columns=None, _where_raw=None, _args=None, _parse_model=True,
                for_update=False, **_where_terms):
        """
        Get one object satisfied given conditions if exists, otherwise return `None`.
        :param t: connection context, None to use default
        :param return_columns: return columns tuple, None to return all fields in mapping table
        :param _where_raw: where condition tuple, each element give a condition and combined with `AND`
        :param _args: argument dict for SQL rendering
        :param _parse_model: True to parse result to a list of ORM model objects, False to get list of dict objects
        :param for_update: Is select for update
        :param _where_terms: where condition terms, only equal condition support only, combined with `AND`
        :return: a ORM model object, or None if not found
        """
        return (SelectQuery(cls, columns=return_columns)
                .set_session(model_db_conf=cls._DB_CONF, dbi=t)
                .where_raw(*_where_raw if _where_raw else [])
                .where(**_where_terms)
                .limit(1)
                .for_update(for_update)
                .only(parse_model=_parse_model, args=_args))


class SqlQuery(metaclass=ABCMeta):
    __metaclass__ = ABCMeta

    _KW_TABLE = "{{__RIKO_TABLE__}}"
    _KW_INSERT_REPLACE = "{{__RIKO_INSERT_REPLACE__}}"
    _KW_FIELDS = "{{__RIKO_FIELDS__}}"
    _KW_VALUES = "{{__RIKO_VALUES__}}"
    _KW_ON_DUPLICATE_KEY_UPDATE = "{{__RIKO_DUPLICATE_KEY__}}"
    _KW_JOIN = "{{__RIKO_JOIN__}}"
    _KW_WHERE = "{{__RIKO_WHERE__}}"
    _KW_DISTINCT = "{{__RIKO_DISTINCT__}}"
    _KW_GROUP_BY = "{{__RIKO_GROUP_BY__}}"
    _KW_HAVING = "{{__RIKO_HAVING__}}"
    _KW_ORDER_BY = "{{__RIKO_ORDER_BY__}}"
    _KW_LIMIT = "{{__RIKO_LIMIT__}}"
    _KW_OFFSET = "{{__RIKO_OFFSET__}}"
    _KW_FORUPDATE = "{{__RIKO_FOR_UPDATE__}}"

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
{{__RIKO_JOIN__}}
{{__RIKO_WHERE__}}
{{__RIKO_GROUP_BY__}}
{{__RIKO_HAVING__}}
{{__RIKO_ORDER_BY__}}
{{__RIKO_LIMIT__}}
{{__RIKO_OFFSET__}}
{{__RIKO_FOR_UPDATE__}}
"""

    def __init__(self, clazz):
        assert clazz is not None
        self._sql = None
        self._dbi = None
        self._clz_meta = clazz
        self._temporary_dbi = False
        self._args = dict()
        self._is_batch = False

    def __str__(self):
        return self._sql

    def set_session(self, model_db_conf, dbi):
        """
        Binding db session for ORM operations.
        :param model_db_conf: model _DB_CONF
        :param dbi: DBI object
        """
        if dbi is None:
            if model_db_conf is None:
                model_db_conf = Riko.db_config
            self._dbi = DBI(model_db_conf)
            self._temporary_dbi = True
        else:
            self._dbi = dbi
        return self

    def get(self, args=None, parse_model=False):
        """
        Execute and get result of query.
        :param args: argument dict for SQL rendering
        :param parse_model: True to parse result to a list of ORM model objects, False to get list of dict objects
        :return: see `parse_model` parameter description
        """
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

    def only(self, parse_model=False, args=None):
        """
        Execute and get result of query, but only one object will be returned.
        :param args: argument dict for SQL rendering
        :param parse_model: True to parse result to a list of ORM model objects, False to get list of dict objects
        :return: a ORM model object, or None if not found
        """
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        try:
            ret = self._dbi.query(self._sql, self._args)
            ret_raw = ret[0] if ret and len(ret) > 0 else None
            if parse_model is False or ret_raw is None:
                return ret_raw
            else:
                return self._clz_meta.deserialize(self._dbi.get_config(), **ret_raw)
        finally:
            if self._temporary_dbi:
                self._dbi.close()

    def go(self, args=None, return_last_id=False):
        """
        Execute the query.
        :param args: argument dict for SQL rendering
        :param return_last_id: True to return last insert id, False to return affected row count
        :return: see `return_last_id` parameter description
        """
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        try:
            if self._is_batch is False:
                return self._dbi.query(self._sql, self._args,
                                       return_pattern=DBI.RETURN_AFFECTED_ROW
                                       if return_last_id is False else DBI.RETURN_LAST_ROW_ID)
            else:
                return self._dbi.insert_many(self._sql, self._args)
        finally:
            if self._temporary_dbi:
                self._dbi.close()

    def cursor(self, args=None):
        """
        Execute the query and get result fetching cursor, it should be close by yourself.
        :param args: argument dict for SQL rendering
        :return: execution result fetching cursor
        """
        self._prepare_sql()
        if args is not None:
            self._args.update(args)
        return self._dbi.query(self._sql, self._args, return_pattern=DBI.RETURN_NONE)

    @contextlib.contextmanager
    def with_cursor(self, args=None):
        """
        Execute the query and get result fetching cursor in a context scope.
        :param args: argument dict for SQL rendering
        """
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
    def _prepare_sql(self):
        pass


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

    def where_raw(self, *condition_terms):
        """
        Set WHERE condition by given pattern, combined with `AND`.
        :param condition_terms: strings to describe where condition, like "id < 5"
        """
        self._where.extend(condition_terms)
        return self

    def where(self, **equal_condition_terms):
        """
        Set WHERE condition by given pattern, combined with `AND`.
        :param equal_condition_terms: key-value pattern to describe where condition, like `id=5`
        """
        for (k, v) in equal_condition_terms.items():
            self._where.append(k + " = %(__RIKO_WHERE_" + k + ")s")
            self._args["__RIKO_WHERE_" + k] = v
        return self

    def _construct_where_clause(self):
        if len(self._where) == 0:
            return ""
        return "WHERE " + " AND ".join(self._where)

    @abstractmethod
    def _prepare_sql(self):
        pass


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
        """
        Set ORDER BY fields by given pattern.
        :param column_terms: strings to describe order principal, like "id", "update_time DESC"
        """
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
    def _prepare_sql(self):
        pass


class PaginationOrderQuery(OrderedQuery):
    def __init__(self, clazz, where=None, limit=None, offset=None, order_by=None):
        super().__init__(clazz, where, order_by)
        self._limit = limit
        self._offset = offset

    def pagination(self, page, per_page):
        """
        Paginate the query result.
        :param page: page number
        :param per_page: record number per page
        """
        self._offset = page * per_page
        self._limit = per_page
        return self

    def limit(self, limit):
        """
        Limit the max result row count of query result.
        :param limit: max fetched record number
        """
        self._limit = limit
        return self

    def offset(self, offset):
        """
        Set offset to fetch query result.
        :param offset: offset to begin fetching record
        """
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
    def _prepare_sql(self):
        pass


class InsertQuery(SqlQuery):
    def __init__(self, clazz):
        super().__init__(clazz)
        self._on_duplicate_key_ignore = False
        self._on_duplicate_key_replace = False
        self._insert_fields = list()
        self._duplicate_update = list()

    def on_duplicate_key_update_raw(self, *update_terms):
        """
        Set update clause for ON DUPLICATE KEY UPDATE. Prior than EXCEPTION.
        :param update_terms: strings for describe how to update, like "age = age + 1"
        """
        self._duplicate_update.extend(update_terms)
        return self

    def on_duplicate_key_update(self, **update_terms):
        """
        Set update clause for ON DUPLICATE KEY UPDATE. Prior than EXCEPTION.
        :param update_terms: key-value pair to describe how to update, like `age=my_object.age + 1`
        """
        for (k, v) in update_terms.items():
            self._duplicate_update.append(k + " = %(__RIKO_UPSERT_" + k + ")s")
            self._args["__RIKO_UPSERT_" + k] = v
        return self

    def ignore(self, is_ignore=True):
        """
        Set ignore when duplicated key. Prior than UPDATE.
        :param is_ignore: if ignore current query when duplicated key
        """
        self._on_duplicate_key_ignore = is_ignore
        return self

    def replace(self, is_replace=True):
        """
        Set ignore when duplicated key. Prior than IGNORE.
        :param is_replace: if replace old record by current record when duplicated key
        """
        self._on_duplicate_key_replace = is_replace
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

    def _construct_on_duplicate_key_update_clause(self):
        if self._on_duplicate_key_ignore is True or \
                self._on_duplicate_key_replace is True or \
                len(self._duplicate_update) == 0:
            return ""
        return "ON DUPLICATE KEY UPDATE " + ", ".join(self._duplicate_update)

    def _prepare_sql(self):
        pass


class SingleInsertQuery(InsertQuery):
    def __init__(self, clazz):
        super().__init__(clazz)
        self._insert_values = list()

    def values_raw(self, insert_field_terms, insert_value_terms):
        """
        Set insert fields and values.
        :param insert_field_terms: tuple/list of fields to set, like ("username", "age")
        :param insert_value_terms: tuple/list of values to set, like ("Nanami Touko", 17)
        """
        if insert_field_terms is None or insert_value_terms is None:
            return self
        assert len(insert_field_terms) == len(insert_value_terms)
        if type(insert_field_terms) in (list, tuple):
            self._insert_fields.extend(insert_field_terms)
            self._insert_values.extend(insert_value_terms)
        else:
            self._insert_fields.append(str(insert_field_terms))
            self._insert_values.append(str(insert_value_terms))

    def values(self, **insert_terms):
        """
        Set insert fields and values.
        :param insert_terms: key-value pair to give field and its value, like `username="Nanami Touko", age=17`
        """
        for (k, v) in insert_terms.items():
            self._insert_fields.append(k)
            self._insert_values.append("%(__RIKO_VALUES_" + k + ")s")
            self._args["__RIKO_VALUES_" + k] = v
        return self

    def _construct_insert_values_clause(self):
        assert len(self._insert_values) > 0
        return ", ".join(self._insert_values)

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


class BatchInsertQuery(InsertQuery):
    def __init__(self, clazz):
        super().__init__(clazz)
        self._insert_value_tuples = list()
        self._is_batch = True

    def values(self, insert_field_terms, insert_value_terms_many):
        """
        Set insert fields and values of multiple data rows.
        :param insert_field_terms: tuple/list of fields, like ("username", "age")
        :param insert_value_terms_many: list of tuples of value, like [("Nanami Touko", 17), ("Koito Yuu", 16)]
        """
        if insert_field_terms is None or insert_value_terms_many is None:
            return self
        assert type(insert_field_terms) in (list, tuple)
        assert type(insert_value_terms_many) in (list, tuple)
        self._insert_fields = list(insert_field_terms)
        self._insert_value_tuples.extend(insert_value_terms_many)
        return self

    def from_objects(self, insert_objs):
        """
        Set insert ORM object of multiple data rows.
        :param insert_objs: list of objects, like [article1, article2], all objects must be the same type.
        """
        if isinstance(insert_objs, self._clz_meta):
            insert_objs = [insert_objs]
        assert isinstance(insert_objs, list)
        if insert_objs is None or len(insert_objs) == 0:
            return self
        sampled = insert_objs[0]
        assert isinstance(sampled, self._clz_meta)
        self._insert_fields.extend(sampled.get_fields())
        for t in insert_objs:
            assert isinstance(t, self._clz_meta)
            t_terms = list()
            for k in self._insert_fields:
                actual_value = t.get_value(k)
                t_terms.append(actual_value)
            self._insert_value_tuples.append(tuple(t_terms))
        return self

    def _construct_insert_values_clause(self):
        assert len(self._insert_value_tuples) > 0 and len(self._insert_fields) > 0
        placeholder = list()
        for x in range(0, len(self._insert_fields)):
            placeholder.append("%s")
        return ", ".join(placeholder)

    def _prepare_sql(self):
        self._sql = SqlQuery._Insert_Template
        r_dict = {
            SqlQuery._KW_INSERT_REPLACE: self._construct_insert_operator_clause(),
            SqlQuery._KW_TABLE: self._clz_meta.__name__,
            SqlQuery._KW_FIELDS: self._construct_insert_fields_clause(),
            SqlQuery._KW_VALUES: self._construct_insert_values_clause(),
            SqlQuery._KW_ON_DUPLICATE_KEY_UPDATE: self._construct_on_duplicate_key_update_clause()
        }
        self._args = self._insert_value_tuples
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
        """
        Set insert fields and values.
        :param update_terms: strings for describe how to update, like "age = age + 1"
        """
        self._update_set.extend(update_terms)
        return self

    def set(self, **update_terms):
        """
        Set update fields and values.
        :param update_terms: key-value pair to give field and its value, like `username="Nanami Touko", age=17`
        """
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
        self._for_update = False
        self._group_by = list()
        self._having = list()
        self._alias = None
        self._join = list()
        self._join_type = dict()
        self._join_on = dict()

    def alias(self, alias):
        """
        Give a alias name for the main query table.
        :param alias: alias table name
        """
        self._alias = alias
        return self

    def for_update(self, is_for_update=True):
        """
        Set SELECT FOR UPDATE for query.
        :param is_for_update: is select for update mode, default True
        """
        self._for_update = is_for_update
        return self

    def distinct(self, is_distinct=True):
        """
        Set DISTINCT for query.
        :param is_distinct: is result distinct, default True
        """
        self._distinct = is_distinct
        return self

    def natural_join(self, join_clazz, alias=None):
        """
        Perform natural join to another table.
        :param join_clazz: `type` of another ORM model to be joined.
        :param alias: alias for another model
        """
        actual_join_term = join_clazz.__name__ + (" AS " + alias if alias is not None else "")
        self._join.append(actual_join_term)
        self._join_type[actual_join_term] = "NATURAL"
        return self

    def join(self, join_clazz, join_mode=JOIN.INNER_JOIN, alias=None, on=None, **on_terms):
        """
        Perform join to another table.
        :param join_mode: which join pattern to be performed, default INNER JOIN
        :param join_clazz: `type` of another ORM model to be joined.
        :param alias: alias for another model
        :param on: tuple/list of fields to JOIN ON, like ("id", )
        :param on_terms: key-value pair to describe JOIN ON
        """
        if join_mode == JOIN.NATURAL_JOIN:
            return self.natural_join(join_clazz=join_clazz, alias=alias)
        elif join_mode == JOIN.INNER_JOIN:
            return self.inner_join(join_clazz, alias=alias, on=on, **on_terms)
        elif join_mode == JOIN.LEFT_JOIN:
            return self.left_join(join_clazz, alias=alias, on=on, **on_terms)
        elif join_mode == JOIN.RIGHT_JOIN:
            return self.right_join(join_clazz, alias=alias, on=on, **on_terms)

    def inner_join(self, join_clazz, alias=None, on=None, **on_terms):
        """
        Perform inner join to another table.
        :param join_clazz: `type` of another ORM model to be joined.
        :param alias: alias for another model
        :param on: tuple/list of fields to JOIN ON, like ("id", )
        :param on_terms: key-value pair to describe JOIN ON
        """
        return self.__handle_join("INNER", join_clazz, alias=alias, on=on, **on_terms)

    def left_join(self, join_clazz, alias=None, on=None, **on_terms):
        """
        Perform left outer join to another table.
        :param join_clazz: `type` of another ORM model to be joined.
        :param alias: alias for another model
        :param on: tuple/list of fields to JOIN ON, like ("id", )
        :param on_terms: key-value pair to describe JOIN ON
        """
        return self.__handle_join("LEFT", join_clazz, alias=alias, on=on, **on_terms)

    def right_join(self, join_clazz, alias=None, on=None, **on_terms):
        """
        Perform right outer join to another table.
        :param join_clazz: `type` of another ORM model to be joined.
        :param alias: alias for another model
        :param on: tuple/list of fields to JOIN ON, like ("id", )
        :param on_terms: key-value pair to describe JOIN ON
        """
        return self.__handle_join("RIGHT", join_clazz, alias=alias, on=on, **on_terms)

    def group_by(self, group_terms):
        """
        Set GROUP BY fields by given pattern.
        :param group_terms: list/tuple to describe group principal, like "author" or ("country", "province")
        """
        if group_terms is not None:
            if type(group_terms) in (list, tuple):
                self._group_by.extend(group_terms)
            else:
                self._group_by.append(str(group_terms))
        return self

    def having_raw(self, having_terms):
        """
        Set HAVING terms by given pattern.
        :param having_terms: list/tuple to describe HAVING principal, like "count(*) < 3"
        """
        self._having.extend(having_terms)
        return self

    def having(self, **having_terms):
        """
        Set HAVING terms by given pattern.
        :param having_terms: key-value pair to describe HAVING principal, like `id=3, title="wow"`
        """
        for (k, v) in having_terms.items():
            self._having.append(k + " = %(__RIKO_HAVING_" + k + ")s")
            self._args["__RIKO_HAVING_" + k] = v
        return self

    def __handle_join(self, join_type, join_clazz, alias=None, on=None, **on_terms):
        actual_join_term = join_clazz.__name__ + (" AS " + alias if alias is not None else "")
        self._join.append(actual_join_term)
        self._join_on[actual_join_term] = list()
        if on is not None:
            if type(on) in (list, tuple):
                self._join_on[actual_join_term].extend(on)
            else:
                self._join_on[actual_join_term].append(str(on))
        for (k, v) in on_terms.items():
            self._join_on[actual_join_term].append(k + " = %(__RIKO_" + join_type + "_ON_" + k + ")s")
            self._args["__RIKO_" + join_type + "_ON_" + k] = v
        self._join_type[actual_join_term] = join_type
        return self

    def _construct_alias_table_clause(self):
        return self._clz_meta.__name__ if self._alias is None else (self._clz_meta.__name__ + " AS " + str(self._alias))

    def _construct_distinct_clause(self):
        return "DISTINCT" if self._distinct else ""

    def _construct_for_update_clause(self):
        return "FOR UPDATE" if self._for_update else ""

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

    def _construct_join_clause(self):
        join_clause = ""
        for join_term in self._join:
            if self._join_type[join_term] == "NATURAL":
                join_clause += " NATURAL JOIN " + join_term
            elif self._join_type[join_term] == "INNER":
                join_clause += " JOIN " + join_term + " ON " + " AND ".join(self._join_on[join_term])
            elif self._join_type[join_term] == "LEFT":
                join_clause += " LEFT JOIN " + join_term + " ON " + " AND ".join(self._join_on[join_term])
            elif self._join_type[join_term] == "RIGHT":
                join_clause += " RIGHT JOIN " + join_term + " ON " + " AND ".join(self._join_on[join_term])
        return join_clause

    def _prepare_sql(self):
        self._sql = SqlQuery._Select_Template
        r_dict = {
            SqlQuery._KW_DISTINCT: self._construct_distinct_clause(),
            SqlQuery._KW_FIELDS: self._construct_select_fields_clause(),
            SqlQuery._KW_TABLE: self._construct_alias_table_clause(),
            SqlQuery._KW_JOIN: self._construct_join_clause(),
            SqlQuery._KW_WHERE: self._construct_where_clause(),
            SqlQuery._KW_GROUP_BY: self._construct_group_by_clause(),
            SqlQuery._KW_HAVING: self._construct_having_clause(),
            SqlQuery._KW_ORDER_BY: self._construct_order_by_clause(),
            SqlQuery._KW_LIMIT: self._construct_limit_clause(),
            SqlQuery._KW_OFFSET: self._construct_offset_clause(),
            SqlQuery._KW_FORUPDATE: self._construct_for_update_clause(),
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

    # Fields list
    fields = ()

    def __init__(self, _db_config=None):
        """
        Create a new dict-like ORM object.
        :param _db_config: db config using for connect to db, or None to use default `RikoConfig`
        """
        dict.__init__(self)
        AbstractModel.__init__(self, _db_config)

    def get_ak(self):
        if self.ak in self:
            return self[self.ak]
        else:
            return None

    def set_ak(self, value):
        self[self.ak] = value

    def get_fields(self):
        return self.fields

    def get_columns(self):
        return tuple(self.pk) + tuple(self.fields)

    def get_value(self, column):
        return self[column] if column in self else None

    def set_value(self, column, value):
        cols = self.get_columns()
        if column in cols:
            self[column] = value
        else:
            raise Exception("Miss match column in Model: " + column)


class ObjectModel(AbstractModel):
    """
    Basic object model in object mapping structure, inherit this and set `pk` and `fields`.
    """

    def __init__(self, _db_config=None):
        super().__init__(_db_config)
        self._model_fields = None
        self._model_columns = None

    def get_ak(self):
        return getattr(self, self.ak)

    def set_ak(self, value):
        if hasattr(self, self.ak):
            setattr(self, self.ak, value)
        else:
            raise Exception("Miss match auto increment column in Model: " + self.ak)

    def get_fields(self):
        if self._model_fields is None:
            self._model_fields = list(vars(self).keys())
            for _ik in self._abstract_inner_var:
                self._model_fields.remove(_ik)
            self._model_fields.remove("_model_fields")
            self._model_fields.remove("_model_columns")
            for pkt in self.pk:
                self._model_fields.remove(pkt)
        return self._model_fields

    def get_columns(self):
        return tuple(self.get_fields()) + tuple(self.pk)

    def get_value(self, column):
        if hasattr(self, column):
            return getattr(self, column)
        else:
            return None

    def set_value(self, column, value):
        if hasattr(self, column):
            setattr(self, column, value)
        else:
            raise Exception("Miss match column in Model: " + column)


class DBI:
    """
    DB connection session.
    """
    RETURN_NONE = 0
    RETURN_CURSOR = 1
    RETURN_RESULT = 2
    RETURN_LAST_ROW_ID = 3
    RETURN_AFFECTED_ROW = 4

    @staticmethod
    def get_connection(db_config=None):
        """
        Get a DBI object by connecting with DB using config.
        :param db_config: DB connection config, None to use default `Riko.db_config`
        :return: DBI object, represent a connection session, not thread safe since using pymysql
        """
        return DBI(Riko.db_config if db_config is None else db_config)

    def __init__(self, db_config):
        assert db_config is not None
        self._db_conf = db_config
        self._conn = pymysql.connect(**db_config)

    def get_config(self):
        """
        Get current session connection config.
        :return: a dict of connection config
        """
        return self._db_conf

    def close(self):
        """
        Close the connection.
        """
        self._conn.close()

    def query(self, sql, args, t=None, return_pattern=RETURN_RESULT):
        """
        Perform a raw query.
        :param sql: sql to perform
        :param args: argument dict for sql rendering
        :param t: connection provider
        :param return_pattern: result return pattern, default `RETURN_RESULT`
        :return: RETURN_RESULT       - a list of dict objects
                 RETURN_CURSOR       - a cursor for fetching result
                 RETURN_LAST_ROW_ID  - inserted record auto increment id
                 RETURN_AFFECTED_ROW - query affected row count
        """
        _conn = t or self._conn
        _reconn = False if t else True
        ret_val = None
        try:
            _conn.ping(reconnect=_reconn)
            cursor = self._conn.cursor()
            # import logging
            # logger = logging.getLogger("ORM_QUERY")
            # logger.info(sql)
            # logger.info(args)
            affected = cursor.execute(sql, args)
            if return_pattern == DBI.RETURN_RESULT:
                fetched = cursor.fetchall()
                # names = [cd[0] for cd in cursor.description]
                # ret_val = [dict(zip(names, v)) for v in fetched]
                ret_val = fetched
            elif return_pattern == DBI.RETURN_CURSOR:
                ret_val = cursor
            elif return_pattern == DBI.RETURN_LAST_ROW_ID:
                ret_val = cursor.lastrowid
            elif return_pattern == DBI.RETURN_AFFECTED_ROW:
                ret_val = affected
        except Exception as ex:
            if not t:
                _conn.rollback()
            raise ex
        else:
            if not t:
                _conn.commit()
            return ret_val

    def insert_many(self, sql_tpl, args, t=None):
        """
        Perform multiple insert query.
        :param sql_tpl: insert sql template
        :param args: args for insert values in tuple in list
        :param t: connection provider
        :return: affected row count
        """
        _conn = t or self._conn
        _reconn = False if t else True
        try:
            _conn.ping(reconnect=_reconn)
            cursor = self._conn.cursor()
            ret_val = cursor.executemany(sql_tpl, args)
        except Exception as ex:
            if not t:
                _conn.rollback()
            raise ex
        else:
            if not t:
                _conn.commit()
            return ret_val

    @contextlib.contextmanager
    def start_transaction(self):
        """
        Create a scoped context with all operations as one transaction.
        """
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
