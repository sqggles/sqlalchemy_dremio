# sqlalchemy_dremio/base.py
# Copyright (C) 2007-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
# Copyright (C) 2007 Paul Johnston, paj@pajhome.org.uk
# Portions derived from jet2sql.py by Matt Keranen, mksql@yahoo.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Support for the Dremio database.

"""
from sqlalchemy import sql, schema, types, exc, pool
from sqlalchemy.sql import compiler, expression
from sqlalchemy.engine import default, base, reflection
from sqlalchemy.engine import url as sa_url
from sqlalchemy import processors
from sqlalchemy import VARCHAR, INTEGER, FLOAT, DATE, TIMESTAMP, TIME, Interval, DECIMAL, LargeBinary, BIGINT, SMALLINT
import sqlalchemy
import platform
import re

_type_map = {
    'boolean': types.BOOLEAN,
    'BOOLEAN': types.BOOLEAN,
    'varbinary': types.LargeBinary,
    'VARBINARY': types.LargeBinary,
    'date': types.DATE,
    'DATE': types.DATE,
    'float': types.FLOAT,
    'FLOAT': types.FLOAT,
    'decimal': types.DECIMAL,
    'DECIMAL': types.DECIMAL,
    'double': types.FLOAT,
    'DOUBLE': types.FLOAT,
    'interval': types.Interval,
    'INTERVAL': types.Interval,
    'int': types.INTEGER,
    'INT': types.INTEGER,
    'integer': types.INTEGER,
    'INTEGER': types.INTEGER,
    'bigint': types.BIGINT,
    'BIGINT': types.BIGINT,
    'time': types.TIME,
    'TIME': types.TIME,
    'timestamp': types.TIMESTAMP,
    'TIMESTAMP': types.TIMESTAMP,
    'varchar': types.VARCHAR,
    'VARCHAR': types.VARCHAR,
    'smallint': types.SMALLINT,
    'CHARACTER VARYING': types.VARCHAR,
    'ANY': types.VARCHAR
}

class DremioExecutionContext(default.DefaultExecutionContext):
    pass


class DremioCompiler(compiler.SQLCompiler):
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))

    def visit_table(self, table, asfrom=False, **kwargs):

        if asfrom:
            if table.schema != "":
                fixed_schema = ".".join(["\"" + i.replace('"', '') + "\"" for i in table.schema.split(".")])
                fixed_table = fixed_schema + ".\"" + table.name.replace("\"", "") + "\""
            else:
                fixed_table = "\"" + table.name.replace("\"", "") + "\""
            return fixed_table
        else:
            return ""


    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        print( tablesample)

class DremioDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler.process(column.type)
        if column is column.table._autoincrement_column and \
            True and \
            (
                column.default is None or \
                isinstance(column.default, schema.Sequence)
            ):
            colspec += " IDENTITY"
            if isinstance(column.default, schema.Sequence) and \
                column.default.start > 0:
                colspec += " " + str(column.default.start)
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

class DremioIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    dremio_reserved = {'abs', 'all', 'allocate', 'allow', 'alter', 'and', 'any', 'are', 'array',
    'array_max_cardinality', 'as', 'asensitivelo', 'asymmetric', 'at', 'atomic', 'authorization',
    'avg', 'begin', 'begin_frame', 'begin_partition', 'between', 'bigint', 'binary', 'bit', 'blob',
    'boolean', 'both', 'by', 'call', 'called', 'cardinality', 'cascaded', 'case', 'cast', 'ceil',
    'ceiling', 'char', 'char_length', 'character', 'character_length', 'check', 'classifier',
    'clob', 'close', 'coalesce', 'collate', 'collect', 'column', 'commit', 'condition', 'connect',
    'constraint', 'contains', 'convert', 'corr', 'corresponding', 'count', 'covar_pop',
    'covar_samp', 'create', 'cross', 'cube', 'cume_dist', 'current', 'current_catalog',
    'current_date', 'current_default_transform_group', 'current_path', 'current_role',
    'current_row', 'current_schema', 'current_time', 'current_timestamp',
    'current_transform_group_for_type', 'current_user', 'cursor', 'cycle', 'date', 'day',
    'deallocate', 'dec', 'decimal', 'declare', 'default', 'define', 'delete', 'dense_rank',
    'deref', 'describe', 'deterministic', 'disallow', 'disconnect', 'distinct', 'double', 'drop',
    'dynamic', 'each', 'element', 'else', 'empty', 'end', 'end-exec', 'end_frame', 'end_partition',
    'equals', 'escape', 'every', 'except', 'exec', 'execute', 'exists', 'exp', 'explain', 'extend',
    'external', 'extract', 'false', 'fetch', 'filter', 'first_value', 'float', 'floor', 'for',
    'foreign', 'frame_row', 'free', 'from', 'full', 'function', 'fusion', 'get', 'global', 'grant',
    'group', 'grouping', 'groups', 'having', 'hold', 'hour', 'identity', 'import', 'in',
    'indicator', 'initial', 'inner', 'inout', 'insensitive', 'insert', 'int', 'integer',
    'intersect', 'intersection', 'interval', 'into', 'is', 'join', 'lag', 'language', 'large',
    'last_value', 'lateral', 'lead', 'leading', 'left', 'like', 'like_regex', 'limit', 'ln',
    'local', 'localtime', 'localtimestamp', 'lower', 'match', 'matches', 'match_number',
    'match_recognize', 'max', 'measures', 'member', 'merge', 'method', 'min', 'minute', 'mod',
    'modifies', 'module', 'month', 'more', 'multiset', 'national', 'natural', 'nchar', 'nclob',
    'new', 'next', 'no', 'none', 'normalize', 'not', 'nth_value', 'ntile', 'null', 'nullif',
    'numeric', 'occurrences_regex', 'octet_length', 'of', 'offset', 'old', 'omit', 'on', 'one',
    'only', 'open', 'or', 'order', 'out', 'outer', 'over', 'overlaps', 'overlay', 'parameter',
    'partition', 'pattern', 'per', 'percent', 'percentile_cont', 'percentile_disc', 'percent_rank',
    'period', 'permute', 'portion', 'position', 'position_regex', 'power', 'precedes', 'precision',
    'prepare', 'prev', 'primary', 'procedure', 'range', 'rank', 'reads', 'real', 'recursive',
    'ref', 'references', 'referencing', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept',
    'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'regr_syy', 'release', 'reset', 'result',
    'return', 'returns', 'revoke', 'right', 'rollback', 'rollup', 'row', 'row_number', 'rows',
    'running', 'savepoint', 'scope', 'scroll', 'search', 'second', 'seek', 'select', 'sensitive',
    'session_user', 'set', 'minus', 'show', 'similar', 'skip', 'smallint', 'some', 'specific',
    'specifictype', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning', 'sqrt', 'start', 'static',
    'stddev_pop', 'stddev_samp', 'stream', 'submultiset', 'subset', 'substring', 'substring_regex',
    'succeeds', 'sum', 'symmetric', 'system', 'system_time', 'system_user', 'table', 'tablesample',
    'then', 'time', 'timestamp', 'timezone_hour', 'timezone_minute', 'tinyint', 'to', 'trailing',
    'translate', 'translate_regex', 'translation', 'treat', 'trigger', 'trim', 'trim_array',
    'true', 'truncate', 'uescape', 'union', 'unique', 'unknown', 'unnest', 'update', 'upper',
    'upsert', 'user', 'using', 'value', 'values', 'value_of', 'var_pop', 'var_samp', 'varbinary',
    'varchar', 'varying', 'versioning', 'when', 'whenever', 'where', 'width_bucket', 'window',
    'with', 'within', 'without', 'year'}

    dremio_unique = dremio_reserved - reserved_words
    reserved_words.update(list(dremio_unique))

    def __init__(self, dialect):
        super(DremioIdentifierPreparer, self).\
                __init__(dialect, initial_quote='"', final_quote='"')

class DremioDialect(default.DefaultDialect):
    name = 'dremio'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    poolclass = pool.SingletonThreadPool
    statement_compiler = DremioCompiler
    ddl_compiler = DremioDDLCompiler
    preparer = DremioIdentifierPreparer
    execution_ctx_cls = DremioExecutionContext

    @classmethod
    def dbapi(cls):
        import pyodbc as module
        return module

    def connect(self, *cargs, **cparams):
        sa_args = cargs[0].split(';')
        engine_args = [arg for arg in sa_args]
        engine_params = [param.lower() for param in cparams.keys()]

        if 'autocommit' not in engine_params:
            cparams['autocommit'] = 1

        clean_args = engine_args

        dsn_regex = re.compile("dsn=.*", re.IGNORECASE)
        if list(filter(dsn_regex.match, engine_args)):
            extra_params_regex = re.compile('(host=|port=|schema=).*', re.IGNORECASE)
            clean_args = [arg for arg in engine_args if not extra_params_regex.match(arg)]

        auth_regex = re.compile('authenticationtype=.*', re.IGNORECASE)
        cnx_regex = re.compile('connectiontype=.*', re.IGNORECASE)
        trusted_regex = re.compile('trusted_connection=yes', re.IGNORECASE)

        if not list(filter(auth_regex.match, clean_args)):
            clean_args.append('AuthenticationType=Plain')
        if not list(filter(cnx_regex.match, clean_args)):
            clean_args.append('ConnectionType=Direct')
        if list(filter(trusted_regex.match, engine_args)):
            clean_args.append('UID=admin')
            clean_args.append('PWD=password1')

        cargs = tuple([';'.join(clean_args)], )

        return self.dbapi.connect(*cargs, **cparams)

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def get_indexes(self, connection, table_name, schema, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_columns(self, connection, table_name, schema, **kw):

        q = "DESCRIBE \"{0}\".\"{1}\"".format(schema, table_name)
        cursor = connection.execute(q)
        result = []
        for col in cursor:
            cname = col[0]
            ctype = _type_map[col[1]]
            bisnull = True
            column = {
                "name": cname,
                "type": ctype,
                "default": None,
                "comment": None,
                "nullable": True
            }
            result.append(column)
        return(result)

    @reflection.cache
    def get_table_names(self, connection, schema, **kw):
        sql = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES"'
        params = {}
        if schema is not None:
            params["schema"] = schema
            sql += " WHERE TABLE_SCHEMA = :schema"

        result = connection.execute(sql, **params)
        table_names = [r[0] for r in result]
        return table_names

    def get_schema_names(self, connection, schema=None, **kw):
        result = connection.execute("SHOW SCHEMAS")
        schema_names = [r[0] for r in result]
        return schema_names
