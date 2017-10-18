# -*- coding: utf-8 -*-

"""Top-level package for SQLAlchemy Dremio."""

__author__ = """Ajish George"""
__email__ = 'yell@aji.sh'
__version__ = '0.1.0'

# Dremio supported data types: https://docs.dremio.com/sql-reference/data-types.html
# BOOLEAN  # VARBINARY  # DATE       # FLOAT    
# DECIMAL  # DOUBLE     # INTERVAL   # INT    
# BIGINT   # TIME       # TIMESTAMP  # VARCHAR    
# MAP      # LIST    

# TODO: figure out how to handle DOUBLE properly
# TODO: figure out how to deal with LIST and MAP types

from sqlalchemy.dialects import registry

registry.register("access", "sqlalchemy_access.pyodbc", "AccessDialect_pyodbc")
registry.register("access.pyodbc", "sqlalchemy_access.pyodbc", "AccessDialect_pyodbc")
