# sqlalchemy_dremio/pyodbc.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Support for Dremio via pyodbc.

pyodbc is available at:

    http://pypi.python.org/pypi/pyodbc/

Connecting
^^^^^^^^^^

Examples of pyodbc connection string URLs:

* ``dremio+pyodbc://mydsn`` - connects using the specified DSN named ``mydsn``.

"""


from .base import DremioExecutionContext, DremioDialect
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy import types as sqltypes, util
from sqlalchemy import util
import decimal
import platform
import re
import six

class _DremioNumeric_pyodbc(sqltypes.Numeric):
    """Turns Decimals with adjusted() < 0 or > 7 into strings.

    The routines here are needed for older pyodbc versions
    as well as current mxODBC versions.

    """

    def bind_processor(self, dialect):

        super_process = super(_DremioNumeric_pyodbc, self).\
                        bind_processor(dialect)

        if not dialect._need_decimal_fix:
            return super_process

        def process(value):
            if self.asdecimal and \
                    isinstance(value, decimal.Decimal):

                adjusted = value.adjusted()
                if adjusted < 0:
                    return self._small_dec_to_string(value)
                elif adjusted > 7:
                    return self._large_dec_to_string(value)

            if super_process:
                return super_process(value)
            else:
                return value
        return process

    # these routines needed for older versions of pyodbc.
    # as of 2.1.8 this logic is integrated.

    def _small_dec_to_string(self, value):
        return "%s0.%s%s" % (
                    (value < 0 and '-' or ''),
                    '0' * (abs(value.adjusted()) - 1),
                    "".join([str(nint) for nint in value.as_tuple()[1]]))

    def _large_dec_to_string(self, value):
        _int = value.as_tuple()[1]
        if 'E' in str(value):
            result = "%s%s%s" % (
                    (value < 0 and '-' or ''),
                    "".join([str(s) for s in _int]),
                    "0" * (value.adjusted() - (len(_int)-1)))
        else:
            if (len(_int) - 1) > value.adjusted():
                result = "%s%s.%s" % (
                (value < 0 and '-' or ''),
                "".join(
                    [str(s) for s in _int][0:value.adjusted() + 1]),
                "".join(
                    [str(s) for s in _int][value.adjusted() + 1:]))
            else:
                result = "%s%s" % (
                (value < 0 and '-' or ''),
                "".join(
                    [str(s) for s in _int][0:value.adjusted() + 1]))
        return result


class DremioExecutionContext_pyodbc(DremioExecutionContext):
    pass


class DremioDialect_pyodbc(PyODBCConnector, DremioDialect):
    execution_ctx_cls = DremioExecutionContext_pyodbc
    driver_for_platf = {
        'Linux 64bit': '/opt/dremio-odbc/lib64/libdrillodbc_sb64.so',
        'Linux 32bit': '/opt/dremio-odbc/lib32/libdrillodbc_sb32.so',
        'Windows' : 'Dremio Connector',
        'Darwin': 'Dremio Connector'
    }
    platf = platform.system() + (' ' + platform.architecture()[0] if platform.system() == 'Linux' else '')
    drv = driver_for_platf[platf]
    pyodbc_driver_name = drv
    colspecs = util.update_copy(
        DremioDialect.colspecs,
        {
            sqltypes.Numeric:_DremioNumeric_pyodbc
        }
    )

    def __init__(self, **kw):
        kw.setdefault('convert_unicode', True)
        super(DremioDialect_pyodbc, self).__init__(**kw)

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)

        keys = opts

        query = url.query

        connect_args = {}
        for param in ('ansi', 'unicode_results', 'autocommit'):
            if param in keys:
                connect_args[param.upper()] = util.asbool(keys.pop(param))

        if 'odbc_connect' in keys:
            connectors = [util.unquote_plus(keys.pop('odbc_connect'))]
        else:
            def check_quote(token):
                if ";" in str(token):
                    token = "'%s'" % token
                return token

            keys = dict(
                (k.lower(), check_quote(v)) for k, v in keys.items()
            )

            dsn_connection = 'dsn' in keys or \
                ('host' in keys and 'database' not in keys)
            if dsn_connection:
                connectors = ['DSN=%s' % (keys.pop('host', '') or
                                          keys.pop('dsn', ''))]
                connectors.extend(
                    [
                        'HOST=',
                        'PORT=',
                        'Schema='
                    ])
            else:
                port = ''
                if 'port' in keys and 'port' not in query:
                    port = '%d' % int(keys.pop('port'))

                connectors = []
                driver = keys.pop('driver', self.pyodbc_driver_name)
                if driver is None:
                    util.warn(
                        "No driver name specified; "
                        "this is expected by PyODBC when using "
                        "DSN-less connections")
                else:
                    connectors.append("DRIVER={%s}" % driver)
                connectors.extend(
                    [
                        'HOST=%s' % keys.pop('host', ''),
                        'PORT=%s' % port,
                        'Schema=%s' % keys.pop('database', '')
                    ])

            user = keys.pop("user", None)
            if user and 'password' in keys:
                connectors.append("UID=%s" % user)
                connectors.append("PWD=%s" % keys.pop('password', ''))
            elif user and 'password' not in keys:
                pass
            else:
                connectors.append("Trusted_Connection=Yes")

            # if set to 'Yes', the ODBC layer will try to automagically
            # convert textual data from your database encoding to your
            # client encoding.  This should obviously be set to 'No' if
            # you query a cp1253 encoded database from a latin1 client...
            if 'odbc_autotranslate' in keys:
                connectors.append("AutoTranslate=%s" %
                                  keys.pop("odbc_autotranslate"))
            
            connectors.append('INTTYPESINRESULTSIFPOSSIBLE=y')
            connectors.extend(['%s=%s' % (k, v) for k, v in keys.items()])
        return [[";".join(connectors)], connect_args]

        
    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.Error):
            error_codes = {
                '40004', # Connection lost.
                '40009', # Connection lost after internal server error.
                '40018', # Connection lost after system running out of memory.
                '40020', # Connection lost after system running out of memory.
            }
            dremio_error_codes = {
                'HY000': (  # Generic dremio error code
                    re.compile(six.u(r'operation timed out'), re.IGNORECASE),
                    re.compile(six.u(r'connection lost'), re.IGNORECASE),
                    re.compile(six.u(r'Socket closed by peer'), re.IGNORECASE),
                )
            }

            error_code, error_msg = e.args[:2]

            # import pdb; pdb.set_trace()
            if error_code in dremio_error_codes:
                # Check dremio error
                for msg_re in dremio_error_codes[error_code]:
                    if msg_re.search(error_msg):
                        return True

                return False

            # Check Pyodbc error
            return error_code in error_codes


        return super(DremioDialect_pyodbc, self).is_disconnect(e, connection, cursor)