import pyodbc

from mock import Mock

from sqlalchemy.engine import url as sa_url

from sqlalchemy.pool import _ConnectionFairy

from sqlalchemy import testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import eq_

from sqlalchemy_dremio.pyodbc import DremioDialect_pyodbc


class DremioDialect_pyodbcTest(fixtures.TestBase):
    __skip_if__ = (lambda: testing.db.dialect.driver != 'pyodbc',)

    def setup(self):
        self.dialect = DremioDialect_pyodbc()
        self.dialect.dbapi = pyodbc

    def assert_parsed(self, dsn, expected_connector, expected_args):
        url = sa_url.make_url(dsn)
        connector, args = self.dialect.create_connect_args(url)
        eq_(connector, expected_connector)
        eq_(args, expected_args)


    def test_create_connect_args(self):
        self.assert_parsed("dremio+pyodbc://admin:password1@127.0.0.1:31010/submissions",
                 ['DRIVER={'+DremioDialect_pyodbc.pyodbc_driver_name+'};HOST=127.0.0.1;PORT=31010;Schema=submissions;UID=admin;PWD=password1;INTTYPESINRESULTSIFPOSSIBLE=y'],
                 {})

    def test_create_connect_args_with_driver(self):
        self.assert_parsed("dremio+pyodbc://admin:password1@127.0.0.1:31010/submissions?driver=Dremio+ODBC+Driver",
                 ['DRIVER={'+DremioDialect_pyodbc.pyodbc_driver_name+'};HOST=127.0.0.1;PORT=31010;Schema=submissions;UID=admin;PWD=password1;INTTYPESINRESULTSIFPOSSIBLE=y'],
                 {})

    def test_create_connect_args_dsn(self):
        self.assert_parsed("dremio+pyodbc://admin:password1@dremio_test",
                 ['DSN=dremio_test;HOST=;PORT=;Schema=;UID=admin;PWD=password1;INTTYPESINRESULTSIFPOSSIBLE=y'],
                 {})

    def test_create_connect_args_trusted(self):
        self.assert_parsed("dremio+pyodbc://127.0.0.1:31010/submissions",
                 ['DRIVER={'+DremioDialect_pyodbc.pyodbc_driver_name+'};HOST=127.0.0.1;PORT=31010;Schema=submissions;Trusted_Connection=Yes;INTTYPESINRESULTSIFPOSSIBLE=y'],
                 {})


    def test_create_connect_args_autotranslate(self):
        self.assert_parsed("dremio+pyodbc://admin:password1@127.0.0.1:31010/submissions?odbc_autotranslate=Yes",
                 ['DRIVER={'+DremioDialect_pyodbc.pyodbc_driver_name+'};HOST=127.0.0.1;PORT=31010;Schema=submissions;UID=admin;PWD=password1;AutoTranslate=Yes;INTTYPESINRESULTSIFPOSSIBLE=y'],
                 {})


    def test_create_connect_args_with_param(self):
        self.assert_parsed("dremio+pyodbc://admin:password1@127.0.0.1:31010/submissions?autocommit=true",
                 ['DRIVER={Dremio ODBC Driver};HOST=127.0.0.1;PORT=31010;Schema=submissions;UID=admin;PWD=password1;INTTYPESINRESULTSIFPOSSIBLE=y'],
                 {'AUTOCOMMIT': True})


    def test_create_connect_args_with_param_multiple(self):
        self.assert_parsed("dremio+pyodbc://admin:password1@127.0.0.1:31010/submissions?autocommit=true&ansi=false&unicode_results=false",
                 ['DRIVER={'+DremioDialect_pyodbc.pyodbc_driver_name+'};HOST=127.0.0.1;PORT=31010;Schema=submissions;UID=admin;PWD=password1;INTTYPESINRESULTSIFPOSSIBLE=y'],
                 {'AUTOCOMMIT': True, 'ANSI': False, 'UNICODE_RESULTS': False})


    def test_create_connect_args_with_unknown_params(self):
        self.assert_parsed("dremio+pyodbc://admin:password1@127.0.0.1:31010/submissions?clientname=test&querytimeout=10",
                 ['DRIVER={'+DremioDialect_pyodbc.pyodbc_driver_name+'};HOST=127.0.0.1;PORT=31010;Schema=submissions;UID=admin;PWD=password1;INTTYPESINRESULTSIFPOSSIBLE=y;clientname=test;querytimeout=10'],
                 {})

    def test_is_disconnect(self):
        connection = Mock(spec=_ConnectionFairy)
        cursor = Mock(spec=pyodbc.Cursor)

        errors = [
            pyodbc.Error(
                'HY000',
                '[HY000] [Dremio][Dremio ODBC Driver]Connection lost in socket read attempt. Operation timed out (-1) (SQLExecDirectW)'
            ),
            pyodbc.Error(
                'HY000',
                '[HY000] [Dremio][Dremio ODBC Driver]Socket closed by peer.'
            ),
        ]

        for error in errors:
            status = self.dialect.is_disconnect(error, connection, cursor)

            eq_(status, True)