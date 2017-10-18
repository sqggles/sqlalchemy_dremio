from sqlalchemy.sql import sqltypes


DECIMAL_PRECISION = 38
DECIMAL_SCALE = 0
DOUBLE_SCALE = 0
VARCHAR_LENGTH = None
VARCHAR_CHARSET = None
TIME_PRECISION = 6
TIMESTAMP_PRECISION = 6
TIME_HASTZ = False
TIMESTAMP_HASTZ = False

class DECIMAL(sqltypes.DECIMAL):

    """ Dremio Decimal/Numeric type """

    def __init__(self, precision=DECIMAL_PRECISION, scale=DECIMAL_SCALE, **kw):
        """ Construct a Decimal type
        :param precision: max number of digits that can be stored (range from 1 thru 38)
        :param scale: number of fractional digits of :param precision: to the
                    right of the decimal point  (range from 0 to :param precision:)
        """
        super(DECIMAL, self).__init__(precision=precision, scale=scale, **kw)

class DOUBLE(sqltypes.REAL):

    """ Dremio DOUBLE type """

    def __init__(self, precision=DECIMAL_PRECISION, scale=DECIMAL_SCALE, **kw):
        """ Construct a Decimal type
        :param precision: max number of digits that can be stored (range from 1 thru 38)
        :param scale: number of fractional digits of :param precision: to the
                    right of the decimal point  (range from 0 to :param precision:)
        """
        super(DOUBLE, self).__init__(precision=precision, scale=scale, **kw)

class VARCHAR(sqltypes.String):

    def __init__(self, length=VARCHAR_LENGTH, charset=VARCHAR_CHARSET, **kwargs):

        """Construct a Varchar

        :param length: Optional 0 to n. If None, LONG is used
        (the longest permissible variable length character string)

        :param charset: optional character set for varchar.

        Note: VARGRAPHIC(n) is equivalent to VARCHAR(n) CHARACTER SET GRAPHIC

        """
        super(VARCHAR, self).__init__(length=length, **kwargs)
        self.charset = charset


class VARBINARY(sqltypes.VARBINARY):

    def __init__(self, length=None, **kwargs):

        """Construct a Varbinary

        :param length: Optional 0 to n. If None, LONG is used
        (the longest permissible variable length character string)

        """
        super(VARBINARY, self).__init__(length=length, **kwargs)


class TIME(sqltypes.TIME):

    def __init__(self, precision=TIME_PRECISION, timezone=TIME_HASTZ, **kwargs):

        """ Construct a TIME stored as UTC in Teradata

        :param precision: optional fractional seconds precision. A single digit
        representing the number of significant digits in the fractional
        portion of the SECOND field. Valid values range from 0 to 6 inclusive.
        The default precision is 6

        :param timezone: If set to True creates a Time WITH TIME ZONE type

        """
        super(TIME, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision


class TIMESTAMP(sqltypes.TIMESTAMP):

    def __init__(self, precision=TIMESTAMP_PRECISION, timezone=TIMESTAMP_HASTZ, **kwargs):
        """ Construct a TIMESTAMP stored as UTC in Teradata

        :param precision: optional fractional seconds precision. A single digit
        representing the number of significant digits in the fractional
        portion of the SECOND field. Valid values range from 0 to 6 inclusive.
        The default precision is 6

        :param timezone: If set to True creates a TIMESTAMP WITH TIME ZONE type

        """
        super(TIMESTAMP, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision



