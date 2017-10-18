from sqlalchemy.dialects import registry

registry.register("dremio", "sqlalchemy_dremio.pyodbc", "DremioDialect_pyodbc")
registry.register("dremio.pyodbc", "sqlalchemy_dremio.pyodbc", "DremioDialect_pyodbc")

from sqlalchemy.testing.plugin.pytestplugin import *
