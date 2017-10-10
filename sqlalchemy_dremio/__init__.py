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

from .types import DECIMAL, TIME, TIMESTAMP, VARBINARY, VARCHAR
from sqlalchemy.sql.sqltypes import (Boolean, Date, Float, Interval,
                                     Integer, BigInteger)
__version__ = '0.0.7'

__all__ = (Boolean, Date, Float, 
           Interval, Integer, BigInteger,
           DECIMAL, TIME, TIMESTAMP, VARCHAR)

