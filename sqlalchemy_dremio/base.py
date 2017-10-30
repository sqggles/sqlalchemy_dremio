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
<<<<<<< HEAD
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
=======

class AcNumeric(types.Numeric):
    def get_col_spec(self):
        return "NUMERIC"

    def bind_processor(self, dialect):
        return processors.to_str

    def result_processor(self, dialect, coltype):
        return None

class AcFloat(types.Float):
    def get_col_spec(self):
        return "FLOAT"

    def bind_processor(self, dialect):
        """By converting to string, we can use Decimal types round-trip."""
        return processors.to_str

class AcInteger(types.Integer):
    def get_col_spec(self):
        return "INTEGER"

class AcTinyInteger(types.Integer):
    def get_col_spec(self):
        return "TINYINT"

class AcSmallInteger(types.SmallInteger):
    def get_col_spec(self):
        return "SMALLINT"

class AcDateTime(types.DateTime):
    def get_col_spec(self):
        return "DATETIME"

class AcDate(types.Date):

    def get_col_spec(self):
        return "DATETIME"

class AcText(types.Text):
    def get_col_spec(self):
        return "MEMO"

class AcString(types.String):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")

class AcUnicode(types.Unicode):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        return None

class AcChar(types.CHAR):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")

class AcBinary(types.LargeBinary):
    def get_col_spec(self):
        return "BINARY"

class AcBoolean(types.Boolean):
    def get_col_spec(self):
        return "YESNO"

class AcTimeStamp(types.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"

class DremioExecutionContext(default.DefaultExecutionContext):

    def get_lastrowid(self):
        self.cursor.execute("SELECT @@identity AS lastrowid")
        return self.cursor.fetchone()[0]


class DremioCompiler(compiler.SQLCompiler):
    extract_map = compiler.SQLCompiler.extract_map.copy()
    extract_map.update({
            'month': 'm',
            'day': 'd',
            'year': 'yyyy',
            'second': 's',
            'hour': 'h',
            'doy': 'y',
            'minute': 'n',
            'quarter': 'q',
            'dow': 'w',
            'week': 'ww'
    })

    def visit_cast(self, cast, **kwargs):
        return cast.clause._compiler_dispatch(self, **kwargs)

    def visit_select_precolumns(self, select):
        """Dremio puts TOP, it's version of LIMIT here """
        s = select.distinct and "DISTINCT " or ""
        if select.limit:
            s += "TOP %s " % (select.limit)
        if select.offset:
            raise exc.InvalidRequestError(
                    'Dremio does not support LIMIT with an offset')
        return s

    def limit_clause(self, select):
        """Limit in dremio is after the select keyword"""
        return ""

    def binary_operator_string(self, binary):
        """Dremio uses "mod" instead of "%" """
        return binary.operator == '%' and 'mod' or binary.operator

    function_rewrites = {'current_date': 'now',
                          'current_timestamp': 'now',
                          'length': 'len',
                          }
    def visit_function(self, func, **kwargs):
        """Dremio function names differ from the ANSI SQL names;
        rewrite common ones"""
        func.name = self.function_rewrites.get(func.name, func.name)
        return super(DremioCompiler, self).visit_function(func)

    def for_update_clause(self, select):
        """FOR UPDATE is not supported by Dremio; silently ignore"""
        return ''

    # Strip schema
    def visit_table(self, table, asfrom=False, **kwargs):
        if asfrom:
            return self.preparer.quote(table.name, table.quote)
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        return ('(' + self.process(join.left, asfrom=True) + \
                (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN ") + \
                self.process(join.right, asfrom=True) + " ON " + \
                self.process(join.onclause) + ')')

    def visit_extract(self, extract, **kw):
        field = self.extract_map.get(extract.field, extract.field)
        return 'DATEPART("%s", %s)' % \
                    (field, self.process(extract.expr, **kw))
>>>>>>> 181bc0b7716ae1e7eb934f6dc01ff08e0e0356b5

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
<<<<<<< HEAD
=======
    colspecs = {
        types.Unicode: AcUnicode,
        types.Integer: AcInteger,
        types.SmallInteger: AcSmallInteger,
        types.Numeric: AcNumeric,
        types.Float: AcFloat,
        types.DateTime: AcDateTime,
        types.Date: AcDate,
        types.String: AcString,
        types.LargeBinary: AcBinary,
        types.Boolean: AcBoolean,
        types.Text: AcText,
        types.CHAR: AcChar,
        types.TIMESTAMP: AcTimeStamp,
    }
>>>>>>> 181bc0b7716ae1e7eb934f6dc01ff08e0e0356b5
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
<<<<<<< HEAD
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
=======
        connectors = ["Driver={Dremio Driver (*.mdb)}"]
        connectors.append("Dbq=%s" % opts["database"])
        user = opts.get("username", None)
        if user:
            connectors.append("UID=%s" % user)
            connectors.append("PWD=%s" % opts.get("password", ""))
        return [[";".join(connectors)], {}]
>>>>>>> 181bc0b7716ae1e7eb934f6dc01ff08e0e0356b5

    def last_inserted_ids(self):
        return self.context.last_inserted_ids

<<<<<<< HEAD
    def has_table(self, connection, tablename, schema=None):
        result = connection.scalar(
                        sql.text(
                            "select * from INFORMATION_SCHEMA.`TABLES` where "
                            "name=:name"), name=tablename
=======

    def has_table(self, connection, tablename, schema=None):
        result = connection.scalar(
                        sql.text(
                            "select count(*) from msysobjects where "
                            "type=1 and name=:name"), name=tablename
>>>>>>> 181bc0b7716ae1e7eb934f6dc01ff08e0e0356b5
                        )
        return bool(result)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
<<<<<<< HEAD
        result = connection.execute("SHOW TABLES FROM INFORMATION_SCHEMA")
=======
        result = connection.execute("select name from msysobjects where "
                "type=1 and name not like 'MSys%'")
>>>>>>> 181bc0b7716ae1e7eb934f6dc01ff08e0e0356b5
        table_names = [r[0] for r in result]
        return table_names
