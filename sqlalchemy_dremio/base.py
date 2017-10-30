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
from sqlalchemy import processors
from sqlalchemy import VARCHAR, INTEGER, FLOAT, DATE, TIMESTAMP, TIME, Interval, DECIMAL, LargeBinary, BIGINT, SMALLINT
import sqlalchemy
import platform

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
    'varchar': types.String,
    'VARCHAR': types.String,
    'smallint': types.SMALLINT,
    'CHARACTER VARYING': types.String,
    'ANY': types.String
}

class DremioExecutionContext(default.DefaultExecutionContext):
    pass


class DremioCompiler(compiler.SQLCompiler): 
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))

    def visit_table(self, table, asfrom=False, **kwargs):

        if asfrom:
            if table.schema != "":
                fixed_schema = ".".join(["`" + i.replace('`', '') + "`" for i in table.schema.split(".")])
                fixed_table = fixed_schema + ".`" + table.name.replace("`", "") + "`"
            else:
                fixed_table = "`" + table.name.replace("`", "") + "`"
            return fixed_table
        else:
            return ""


    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        print( tablesample)

class DremioDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        if column.table is None:
            raise exc.CompileError(
                            "dremio requires Table-bound columns "
                            "in order to generate DDL")

        colspec = self.preparer.format_column(column)
        seq_col = column.table._autoincrement_column
        if seq_col is column:
            colspec += " AUTOINCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)

            if column.nullable is not None and not column.primary_key:
                if not column.nullable or column.primary_key:
                    colspec += " NOT NULL"
                else:
                    colspec += " NULL"

            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

    def visit_drop_index(self, drop):
        index = drop.element
        self.append("\nDROP INDEX [%s].[%s]" % \
                        (index.table.name,
                        self._index_identifier(index.name)))

class DremioIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(['value', 'text'])
    def __init__(self, dialect):
        super(DremioIdentifierPreparer, self).\
                __init__(dialect, initial_quote='[', final_quote=']')



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

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        driver_for_platf = {
            'Linux 64bit': 'Dremio ODBC Driver 64-bit',
            'Linux 32bit': 'Dremio ODBC Driver 32-bit',
            'Windows' : 'Dremio Connector',
            'Darwin': 'Dremio ODBC Driver'
        }
        platf = platform.system() + ' ' + (platform.architecture()[0] if platform.system() == 'Linux' else '')
        drv = driver_for_platf[platf]
        connectors = ['Driver=%s' % drv]
        connectors.append('ConnectionType=Direct')
        connectors.append('HOST=%s' % opts.get('host', ''))
        connectors.append('PORT=%s' % opts.get('port', ''))
        connectors.append('AuthenticationType=Plain')
        connectors.append('UID=%s' % opts.get('username', ''))
        connectors.append('PWD=%s' % opts.get('password', ''))
        return [[';'.join(connectors)], {}]

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

    def has_table(self, connection, tablename, schema=None):
        result = connection.scalar(
                        sql.text(
                            "select * from INFORMATION_SCHEMA.`TABLES` where "
                            "name=:name"), name=tablename
                        )
        return bool(result)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute("SHOW TABLES FROM INFORMATION_SCHEMA")
        table_names = [r[0] for r in result]
        return table_names
