from datetime import (
    timedelta as Timedelta, datetime as Datetime, date, time)
from warnings import warn
import socket
import platform
import getpass
from struct import pack
from decimal import Decimal
from collections import deque, defaultdict
from itertools import count, islice
from uuid import UUID
from copy import deepcopy
from calendar import timegm
from distutils.version import LooseVersion
from struct import Struct
import struct
from time import localtime
import nzpy
from . import handshake, numeric
from json import loads, dumps
from os import getpid
from scramp import ScramClient
import enum
import logging 
from ipaddress import (
    ip_address, IPv4Address, IPv6Address, ip_network, IPv4Network, IPv6Network)
from datetime import timezone as Timezone


# Copyright (c) 2007-2009, Mathieu Fenniak
# Copyright (c) The Contributors
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__ = "Mathieu Fenniak"


ZERO = Timedelta(0)
BINARY = bytes


class Interval():
    """An Interval represents a measurement of time.  In PostgreSQL, an
    interval is defined in the measure of months, days, and microseconds; as
    such, the nzpy interval type represents the same information.

    Note that values of the :attr:`microseconds`, :attr:`days` and
    :attr:`months` properties are independently measured and cannot be
    converted to each other.  A month may be 28, 29, 30, or 31 days, and a day
    may occasionally be lengthened slightly by a leap second.

    .. attribute:: microseconds

        Measure of microseconds in the interval.

        The microseconds value is constrained to fit into a signed 64-bit
        integer.  Any attempt to set a value too large or too small will result
        in an OverflowError being raised.

    .. attribute:: days

        Measure of days in the interval.

        The days value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.

    .. attribute:: months

        Measure of months in the interval.

        The months value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.
    """

    def __init__(self, microseconds=0, days=0, months=0):
        self.microseconds = microseconds
        self.days = days
        self.months = months

    def _setMicroseconds(self, value):
        if not isinstance(value, int):
            raise TypeError("microseconds must be an integer type")
        elif not (min_int8 < value < max_int8):
            raise OverflowError(
                "microseconds must be representable as a 64-bit integer")
        else:
            self._microseconds = value

    def _setDays(self, value):
        if not isinstance(value, int):
            raise TypeError("days must be an integer type")
        elif not (min_int4 < value < max_int4):
            raise OverflowError(
                "days must be representable as a 32-bit integer")
        else:
            self._days = value

    def _setMonths(self, value):
        if not isinstance(value, int):
            raise TypeError("months must be an integer type")
        elif not (min_int4 < value < max_int4):
            raise OverflowError(
                "months must be representable as a 32-bit integer")
        else:
            self._months = value

    microseconds = property(lambda self: self._microseconds, _setMicroseconds)
    days = property(lambda self: self._days, _setDays)
    months = property(lambda self: self._months, _setMonths)

    def __repr__(self):
        return "<Interval %s months %s days %s microseconds>" % (
            self.months, self.days, self.microseconds)

    def __eq__(self, other):
        return other is not None and isinstance(other, Interval) and \
            self.months == other.months and self.days == other.days and \
            self.microseconds == other.microseconds

    def __neq__(self, other):
        return not self.__eq__(other)


class PGType():
    def __init__(self, value):
        self.value = value

    def encode(self, encoding):
        return str(self.value).encode(encoding)


class PGEnum(PGType):
    def __init__(self, value):
        if isinstance(value, str):
            self.value = value
        else:
            self.value = value.value


class PGJson(PGType):
    def encode(self, encoding):
        return dumps(self.value).encode(encoding)


class PGJsonb(PGType):
    def encode(self, encoding):
        return dumps(self.value).encode(encoding)


class PGTsvector(PGType):
    pass


class PGVarchar(str):
    pass


class PGText(str):
    pass


def pack_funcs(fmt):
    struc = Struct('!' + fmt)
    return struc.pack, struc.unpack_from


i_pack, i_unpack = pack_funcs('i')
h_pack, h_unpack = pack_funcs('h')
q_pack, q_unpack = pack_funcs('q')
d_pack, d_unpack = pack_funcs('d')
f_pack, f_unpack = pack_funcs('f')
iii_pack, iii_unpack = pack_funcs('iii')
ii_pack, ii_unpack = pack_funcs('ii')
qii_pack, qii_unpack = pack_funcs('qii')
dii_pack, dii_unpack = pack_funcs('dii')
ihic_pack, ihic_unpack = pack_funcs('ihic')
ci_pack, ci_unpack = pack_funcs('ci')
c_pack, c_unpack = pack_funcs('c')
bh_pack, bh_unpack = pack_funcs('bh')
cccc_pack, cccc_unpack = pack_funcs('cccc')


min_int2, max_int2 = -2 ** 15, 2 ** 15
min_int4, max_int4 = -2 ** 31, 2 ** 31
min_int8, max_int8 = -2 ** 63, 2 ** 63


class Warning(Exception):
    """Generic exception raised for important database warnings like data
    truncations.  This exception is not currently used by nzpy.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class Error(Exception):
    """Generic exception that is the base exception of all other error
    exceptions.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class InterfaceError(Error):
    """Generic exception raised for errors that are related to the database
    interface rather than the database itself.  For example, if the interface
    attempts to use an SSL connection but the server refuses, an InterfaceError
    will be raised.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class DatabaseError(Error):
    """Generic exception raised for errors that are related to the database.
    This exception is currently never raised by nzpy.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class DataError(DatabaseError):
    """Generic exception raised for errors that are due to problems with the
    processed data.  This exception is not currently raised by nzpy.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class OperationalError(DatabaseError):
    """
    Generic exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer. This
    exception is currently never raised by nzpy.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class IntegrityError(DatabaseError):
    """
    Generic exception raised when the relational integrity of the database is
    affected.  This exception is not currently raised by nzpy.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class InternalError(DatabaseError):
    """Generic exception raised when the database encounters an internal error.
    This is currently only raised when unexpected state occurs in the nzpy
    interface itself, and is typically the result of a interface bug.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class ProgrammingError(DatabaseError):
    """Generic exception raised for programming errors.  For example, this
    exception is raised if more parameter fields are in a query string than
    there are available parameters.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class NotSupportedError(DatabaseError):
    """Generic exception raised in case a method or database API was used which
    is not supported by the database.

    This exception is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.
    """
    pass


class ArrayContentNotSupportedError(NotSupportedError):
    """
    Raised when attempting to transmit an array where the base type is not
    supported for binary data transfer by the interface.
    """
    pass


class ArrayContentNotHomogenousError(ProgrammingError):
    """
    Raised when attempting to transmit an array that doesn't contain only a
    single type of object.
    """
    pass


class ArrayDimensionsNotConsistentError(ProgrammingError):
    """
    Raised when attempting to transmit an array that has inconsistent
    multi-dimension sizes.
    """
    pass


def Date(year, month, day):
    """Constuct an object holding a date value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`
    """
    return date(year, month, day)


def Time(hour, minute, second):
    """Construct an object holding a time value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`
    """
    return time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    """Construct an object holding a timestamp value.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`
    """
    return Datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    """Construct an object holding a date value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.date`
    """
    return Date(*localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Construct an objet holding a time value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.time`
    """
    return Time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Construct an object holding a timestamp value from the given ticks value
    (number of seconds since the epoch).

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    :rtype: :class:`datetime.datetime`
    """
    return Timestamp(*localtime(ticks)[:6])


def Binary(value):
    """Construct an object holding binary data.

    This function is part of the `DBAPI 2.0 specification
    <http://www.python.org/dev/peps/pep-0249/>`_.

    """
    return value


FC_TEXT = 0
FC_BINARY = 1


def convert_paramstyle(style, query):
    # I don't see any way to avoid scanning the query string char by char,
    # so we might as well take that careful approach and create a
    # state-based scanner.  We'll use int variables for the state.
    OUTSIDE = 0    # outside quoted string
    INSIDE_SQ = 1  # inside single-quote string '...'
    INSIDE_QI = 2  # inside quoted identifier   "..."
    INSIDE_ES = 3  # inside escaped single-quote string, E'...'
    INSIDE_PN = 4  # inside parameter name eg. :name
    INSIDE_CO = 5  # inside inline comment eg. --

    in_quote_escape = False
    in_param_escape = False
    placeholders = []
    output_query = []
    param_idx = map(lambda x: "$" + str(x), count(1))
    state = OUTSIDE
    prev_c = None
    for i, c in enumerate(query):
        if i + 1 < len(query):
            next_c = query[i + 1]
        else:
            next_c = None

        if state == OUTSIDE:
            if c == "'":
                output_query.append(c)
                if prev_c == 'E':
                    state = INSIDE_ES
                else:
                    state = INSIDE_SQ
            elif c == '"':
                output_query.append(c)
                state = INSIDE_QI
            elif c == '-':
                output_query.append(c)
                if prev_c == '-':
                    state = INSIDE_CO
            elif style == "qmark" and c == "?":
                output_query.append("NULL")
            elif style == "numeric" and c == ":" and next_c not in ':=' \
                    and prev_c != ':':
                # Treat : as beginning of parameter name if and only
                # if it's the only : around
                # Needed to properly process type conversions
                # i.e. sum(x)::float
                output_query.append("$")
            elif style == "named" and c == ":" and next_c not in ':=' \
                    and prev_c != ':':
                # Same logic for : as in numeric parameters
                state = INSIDE_PN
                placeholders.append('')
            elif style == "pyformat" and c == '%' and next_c == "(":
                state = INSIDE_PN
                placeholders.append('')
            elif style in ("format", "pyformat") and c == "%":
                style = "format"
                if in_param_escape:
                    in_param_escape = False
                    output_query.append(c)
                else:
                    if next_c == "%":
                        in_param_escape = True
                    elif next_c == "s":
                        state = INSIDE_PN
                        output_query.append(next(param_idx))
                    else:
                        raise InterfaceError(
                            "Only %s and %% are supported in the query.")
            else:
                output_query.append(c)

        elif state == INSIDE_SQ:
            if c == "'":
                if in_quote_escape:
                    in_quote_escape = False
                else:
                    if next_c == "'":
                        in_quote_escape = True
                    else:
                        state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_QI:
            if c == '"':
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_ES:
            if c == "'" and prev_c != "\\":
                # check for escaped single-quote
                state = OUTSIDE
            output_query.append(c)

        elif state == INSIDE_PN:
            if style == 'named':
                placeholders[-1] += c
                if next_c is None or (not next_c.isalnum() and next_c != '_'):
                    state = OUTSIDE
                    try:
                        pidx = placeholders.index(placeholders[-1], 0, -1)
                        output_query.append("$" + str(pidx + 1))
                        del placeholders[-1]
                    except ValueError:
                        output_query.append("$" + str(len(placeholders)))
            elif style == 'pyformat':
                if prev_c == ')' and c == "s":
                    state = OUTSIDE
                    try:
                        pidx = placeholders.index(placeholders[-1], 0, -1)
                        output_query.append("$" + str(pidx + 1))
                        del placeholders[-1]
                    except ValueError:
                        output_query.append("$" + str(len(placeholders)))
                elif c in "()":
                    pass
                else:
                    placeholders[-1] += c
            elif style == 'format':
                state = OUTSIDE

        elif state == INSIDE_CO:
            output_query.append(c)
            if c == '\n':
                state = OUTSIDE

        prev_c = c

    def make_args(vals):
        return vals

    return ''.join(output_query), make_args


EPOCH = Datetime(2000, 1, 1)
EPOCH_TZ = EPOCH.replace(tzinfo=Timezone.utc)
EPOCH_SECONDS = timegm(EPOCH.timetuple())
INFINITY_MICROSECONDS = 2 ** 63 - 1
MINUS_INFINITY_MICROSECONDS = -1 * INFINITY_MICROSECONDS - 1


# data is 64-bit integer representing microseconds since 2000-01-01
def timestamp_recv_integer(data, offset, length):
    micros = q_unpack(data, offset)[0]
    try:
        return EPOCH + Timedelta(microseconds=micros)
    except OverflowError:
        if micros == INFINITY_MICROSECONDS:
            return 'infinity'
        elif micros == MINUS_INFINITY_MICROSECONDS:
            return '-infinity'
        else:
            return micros


# data is double-precision float representing seconds since 2000-01-01
def timestamp_recv_float(data, offset, length):
    return Datetime.utcfromtimestamp(EPOCH_SECONDS + d_unpack(data, offset)[0])


# data is 64-bit integer representing microseconds since 2000-01-01
def timestamp_send_integer(v):
    return q_pack(
        int((timegm(v.timetuple()) - EPOCH_SECONDS) * 1e6) + v.microsecond)


# data is double-precision float representing seconds since 2000-01-01
def timestamp_send_float(v):
    return d_pack(timegm(v.timetuple()) + v.microsecond / 1e6 - EPOCH_SECONDS)


def timestamptz_send_integer(v):
    # timestamps should be sent as UTC.  If they have zone info,
    # convert them.
    return timestamp_send_integer(
        v.astimezone(Timezone.utc).replace(tzinfo=None))


def timestamptz_send_float(v):
    # timestamps should be sent as UTC.  If they have zone info,
    # convert them.
    return timestamp_send_float(
        v.astimezone(Timezone.utc).replace(tzinfo=None))


# return a timezone-aware datetime instance if we're reading from a
# "timestamp with timezone" type.  The timezone returned will always be
# UTC, but providing that additional information can permit conversion
# to local.
def timestamptz_recv_integer(data, offset, length):
    return (data[offset:offset+length]).decode("utf-8")

def timestamptz_recv_float(data, offset, length):
    return (data[offset:offset+length]).decode("utf-8") 


def interval_send_integer(v):
    microseconds = v.microseconds
    try:
        microseconds += int(v.seconds * 1e6)
    except AttributeError:
        pass

    try:
        months = v.months
    except AttributeError:
        months = 0

    return qii_pack(microseconds, v.days, months)


def interval_send_float(v):
    seconds = v.microseconds / 1000.0 / 1000.0
    try:
        seconds += v.seconds
    except AttributeError:
        pass

    try:
        months = v.months
    except AttributeError:
        months = 0

    return dii_pack(seconds, v.days, months)


def interval_recv_integer(data, offset, length):
    return (data[offset:offset + length]).decode("utf-8") 


def interval_recv_float(data, offset, length):
    return (data[offset:offset + length]).decode("utf-8") 


def int8_recv(data, offset, length):
    return int(data[offset:offset + length])


def int2_recv(data, offset, length):
    return int(data[offset:offset + length])


def int4_recv(data, offset, length):
    return int(data[offset:offset + length])


def float4_recv(data, offset, length):
    return float(data[offset:offset + length])


def float8_recv(data, offset, length):
    return float(data[offset:offset + length])


def bytea_send(v):
    return v


# bytea
def bytea_recv(data, offset, length):
    return data[offset:offset + length]


def uuid_send(v):
    return v.bytes


def uuid_recv(data, offset, length):
    return UUID(bytes=data[offset:offset+length])


def bool_send(v):
    return b"\x01" if v else b"\x00"


NULL = i_pack(-1)

NULL_BYTE = b'\x00'


def null_send(v):
    return NULL


def int_in(data, offset, length):
    return int(data[offset: offset + length])


class Cursor():
    """A cursor object is returned by the :meth:`~Connection.cursor` method of
    a connection. It has the following attributes and methods:

    .. attribute:: arraysize

        This read/write attribute specifies the number of rows to fetch at a
        time with :meth:`fetchmany`.  It defaults to 1.

    .. attribute:: connection

        This read-only attribute contains a reference to the connection object
        (an instance of :class:`Connection`) on which the cursor was
        created.

        This attribute is part of a DBAPI 2.0 extension.  Accessing this
        attribute will generate the following warning: ``DB-API extension
        cursor.connection used``.

    .. attribute:: rowcount

        This read-only attribute contains the number of rows that the last
        ``execute()`` or ``executemany()`` method produced (for query
        statements like ``SELECT``) or affected (for modification statements
        like ``UPDATE``).

        The value is -1 if:

        - No ``execute()`` or ``executemany()`` method has been performed yet
          on the cursor.
        - There was no rowcount associated with the last ``execute()``.
        - At least one of the statements executed as part of an
          ``executemany()`` had no row count associated with it.
        - Using a ``SELECT`` query statement on PostgreSQL server older than
          version 9.
        - Using a ``COPY`` query statement on PostgreSQL server version 8.1 or
          older.

        This attribute is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

    .. attribute:: description

        This read-only attribute is a sequence of 7-item sequences.  Each value
        contains information describing one result column.  The 7 items
        returned for each column are (name, type_code, display_size,
        internal_size, precision, scale, null_ok).  Only the first two values
        are provided by the current implementation.

        This attribute is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
    """

    def __init__(self, connection):
        self._c = connection
        self.arraysize = 1
        self.ps = None
        self._row_count = -1
        self._cached_rows = deque()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def connection(self):
        warn("DB-API extension cursor.connection used", stacklevel=3)
        return self._c

    @property
    def rowcount(self):
        return self._row_count

    description = property(lambda self: self._getDescription())

    def _getDescription(self):
        if self.ps is None:
            return None
        row_desc = self.ps['row_desc']
        if len(row_desc) == 0:
            return None
        columns = []
        for col in row_desc:
            columns.append(
                (col["name"], col["type_oid"], None, None, None, None, None))
        return columns

    ##
    # Executes a database operation.  Parameters may be provided as a sequence
    # or mapping and will be bound to variables in the operation.
    # <p>
    # Stability: Part of the DBAPI 2.0 specification.
    def execute(self, operation, args=None, stream=None):
        """Executes a database operation.  Parameters may be provided as a
        sequence, or as a mapping, depending upon the value of
        :data:`nzpy.paramstyle`.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute.

        :param args:
            If :data:`paramstyle` is ``qmark``, ``numeric``, or ``format``,
            this argument should be an array of parameters to bind into the
            statement.  If :data:`paramstyle` is ``named``, the argument should
            be a dict mapping of parameters.  If the :data:`paramstyle` is
            ``pyformat``, the argument value may be either an array or a
            mapping.

        :param stream: This is a nzpy extension for use with the PostgreSQL
            `COPY
            <http://www.postgresql.org/docs/current/static/sql-copy.html>`_
            command. For a COPY FROM the parameter must be a readable file-like
            object, and for COPY TO it must be writable.

            .. versionadded:: 1.9.11
        """
        try:
            self.stream = stream

            if not self._c.in_transaction and not self._c.autocommit:
               self._c.execute(self, "begin", None)
               self._c.in_transaction = True
            self._c.execute(self, operation, args)
        except AttributeError as e:
            if self._c is None:
                raise InterfaceError("Cursor closed")
            elif self._c._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e
        return self

    def executemany(self, operation, param_sets):
        """Prepare a database operation, and then execute it against all
        parameter sequences or mappings provided.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param operation:
            The SQL statement to execute
        :param parameter_sets:
            A sequence of parameters to execute the statement with. The values
            in the sequence should be sequences or mappings of parameters, the
            same as the args argument of the :meth:`execute` method.
        """
        rowcounts = []
        for parameters in param_sets:
            self.execute(operation, parameters)
            rowcounts.append(self._row_count)

        self._row_count = -1 if -1 in rowcounts else sum(rowcounts)
        return self

    def fetchone(self):
        """Fetch the next row of a query result set.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:
            A row as a sequence of field values, or ``None`` if no more rows
            are available.
        """
        try:
            return next(self)
        except StopIteration:
            return None
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")
        except AttributeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchmany(self, num=None):
        """Fetches the next set of rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :param size:

            The number of rows to fetch when called.  If not provided, the
            :attr:`arraysize` attribute value is used instead.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.  If no more rows are available, an empty sequence
            will be returned.
        """
        try:
            return tuple(
                islice(self, self.arraysize if num is None else num))
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def fetchall(self):
        """Fetches all remaining rows of a query result.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.

        :returns:

            A sequence, each entry of which is a sequence of field values
            making up a row.
        """
        try:
            return tuple(self)
        except TypeError:
            raise ProgrammingError("attempting to use unexecuted cursor")

    def close(self):
        """Closes the cursor.

        This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self._c = None

    def __iter__(self):
        """A cursor object is iterable to retrieve the rows from a query.

        This is a DBAPI 2.0 extension.
        """
        return self

    def setinputsizes(self, sizes):
        """This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, it is not
        implemented by nzpy.
        """
        pass

    def setoutputsize(self, size, column=None):
        """This method is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_, however, it is not
        implemented by nzpy.
        """
        pass

    def __next__(self):
        try:
            return self._cached_rows.popleft()
        except IndexError:
            if self.ps is None:
                raise ProgrammingError("A query hasn't been issued.")
            elif len(self.ps['row_desc']) == 0:
                raise ProgrammingError("no result set")
            else:
                raise StopIteration()


# Message codes
NOTICE_RESPONSE = b"N"
AUTHENTICATION_REQUEST = b"R"
PARAMETER_STATUS = b"S"
BACKEND_KEY_DATA = b"K"
READY_FOR_QUERY = b"Z"
ROW_DESCRIPTION = b"T"
ERROR_RESPONSE = b"E"
DATA_ROW = b"D"
COMMAND_COMPLETE = b"C"
PARSE_COMPLETE = b"1"
BIND_COMPLETE = b"2"
CLOSE_COMPLETE = b"3"
PORTAL_SUSPENDED = b"s"
NO_DATA = b"n"
PARAMETER_DESCRIPTION = b"t"
NOTIFICATION_RESPONSE = b"A"
COPY_DONE = b"c"
COPY_DATA = b"d"
COPY_IN_RESPONSE = b"G"
COPY_OUT_RESPONSE = b"H"
EMPTY_QUERY_RESPONSE = b"I"

BIND = b"B"
PARSE = b"P"
EXECUTE = b"E"
FLUSH = b'H'
SYNC = b'S'
PASSWORD = b'p'
DESCRIBE = b'D'
TERMINATE = b'X'
CLOSE = b'C'


def create_message(code, data=b''):
    return code + i_pack(len(data) + 4) + data


FLUSH_MSG = create_message(FLUSH)
SYNC_MSG = create_message(SYNC)
TERMINATE_MSG = create_message(TERMINATE)
COPY_DONE_MSG = create_message(COPY_DONE)
EXECUTE_MSG = create_message(EXECUTE, NULL_BYTE + i_pack(0))

# DESCRIBE constants
STATEMENT = b'S'
PORTAL = b'P'

# ErrorResponse codes
RESPONSE_SEVERITY = "S"  # always present
RESPONSE_SEVERITY = "V"  # always present
RESPONSE_CODE = "C"  # always present
RESPONSE_MSG = "M"  # always present
RESPONSE_DETAIL = "D"
RESPONSE_HINT = "H"
RESPONSE_POSITION = "P"
RESPONSE__POSITION = "p"
RESPONSE__QUERY = "q"
RESPONSE_WHERE = "W"
RESPONSE_FILE = "F"
RESPONSE_LINE = "L"
RESPONSE_ROUTINE = "R"

IDLE = b"I"
IDLE_IN_TRANSACTION = b"T"
IDLE_IN_FAILED_TRANSACTION = b"E"

class DbosTupleDesc():

	def __init__(self):
        
         self.version           = None
         self.nullsAllowed      = None        
         self.sizeWord          = None
         self.sizeWordSize      = None	
         self.numFixedFields    = None	
         self.numVaryingFields  = None	 
         self.fixedFieldsSize   = None	
         self.maxRecordSize     = None	
         self.numFields         = None	
         self.field_type 	    = []     
         self.field_size  	    = []     
         self.field_trueSize    = [] 	
         self.field_offset      = [] 	
         self.field_physField   = [] 	
         self.field_logField    = [] 	
         self.field_nullAllowed = []		
         self.field_fixedSize   = [] 	
         self.field_springField = [] 	
         self.DateStyle         = None
         self.EuroDates         = None
         self.DBcharset         = None
         self.EnableTime24      = None

#Connection status
CONN_NOT_CONNECTED = 0
CONN_CONNECTED = 1      
CONN_EXECUTING = 2       
CONN_FETCHING = 3        
CONN_CANCELLED = 4 

# External table stuff (copied from nde/client/exttable.h)

EXTAB_SOCK_DATA  = 1  # block of records
EXTAB_SOCK_ERROR = 2  # error message
EXTAB_SOCK_DONE  = 3  # normal wrap-up
EXTAB_SOCK_FLUSH = 4  # Flush the current buffer/data      

# NZ datatype
NzTypeRecAddr = 1 
NzTypeDouble = 2 
NzTypeInt = 3 
NzTypeFloat = 4
NzTypeMoney = 5 
NzTypeDate = 6
NzTypeNumeric = 7
NzTypeTime = 8
NzTypeTimestamp = 9
NzTypeInterval = 10
NzTypeTimeTz = 11
NzTypeBool = 12
NzTypeInt1 = 13
NzTypeBinary = 14
NzTypeChar = 15
NzTypeVarChar = 16
NzDEPR_Text = 17            # OBSOLETE 3.0: BLAST Era Large 'text' Object    
NzTypeUnknown = 18          # corresponds to PG UNKNOWNOID data type - an untyped string literal
NzTypeInt2 = 19
NzTypeInt8 = 20
NzTypeVarFixedChar = 21
NzTypeGeometry = 22
NzTypeVarBinary = 23
NzDEPR_Blob = 24            # OBSOLETE 3.0: BLAST Era Large 'binary' Object
NzTypeNChar = 25
NzTypeNVarChar = 26
NzDEPR_NText = 27           # OBSOLETE 3.0: BLAST Era Large 'nchar text' Object
NzTypeLastEntry = 28        # KEEP THIS ENTRY LAST - used internally to size an array

# this is version of nzpy driver
nzpy_client_version = "11.0.0.0"

dataType = {NzTypeChar: "NzTypeChar", NzTypeVarChar: "NzTypeVarChar", NzTypeVarFixedChar: "NzTypeVarFixedChar", NzTypeGeometry: "NzTypeGeometry", NzTypeVarBinary: "NzTypeVarBinary", NzTypeNChar: "NzTypeNChar", NzTypeNVarChar: "NzTypeNVarChar"}

arr_trans = dict(zip(map(ord, "[] 'u"), list('{}') + [None] * 3))


class Connection():

    # DBAPI Extension: supply exceptions as attributes on the connection
    Warning = property(lambda self: self._getError(Warning))
    Error = property(lambda self: self._getError(Error))
    InterfaceError = property(lambda self: self._getError(InterfaceError))
    DatabaseError = property(lambda self: self._getError(DatabaseError))
    OperationalError = property(lambda self: self._getError(OperationalError))
    IntegrityError = property(lambda self: self._getError(IntegrityError))
    InternalError = property(lambda self: self._getError(InternalError))
    ProgrammingError = property(lambda self: self._getError(ProgrammingError))
    NotSupportedError = property(
        lambda self: self._getError(NotSupportedError))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _getError(self, error):
        warn(
            "DB-API extension connection.%s used" %
            error.__name__, stacklevel=3)
        return error

    def __init__(
            self, user, host, unix_sock, port, database, password, ssl,
            securityLevel, timeout, application_name, max_prepared_statements, datestyle, logLevel, tcp_keepalive):
        self._client_encoding = "utf8"
        self._commands_with_count = (
            b"INSERT", b"DELETE", b"UPDATE"
            )
        self.notifications = deque(maxlen=100)
        self.notices = deque(maxlen=100)
        self.parameter_statuses = deque(maxlen=100)
        self.max_prepared_statements = int(max_prepared_statements)
        
        if logLevel == 0:
            logLevel= logging.DEBUG
        if logLevel == 1:
            logLevel= logging.INFO
        if logLevel == 2:
            logLevel= logging.WARNING
                
        filename = 'nzpy_' + Datetime.now().strftime("%H_%M_%S") + '.log'
        logging.basicConfig(level=logLevel,
                    format='%(asctime)s :%(filename)s:%(lineno)s - %(funcName)25s(): %(levelname)s: %(message)s',
                    filename=filename,
                    filemode='w')
        
        if user is None:
            raise InterfaceError(
                "The 'user' connection parameter cannot be None")

        if isinstance(user, str):
            self.user = user.encode('utf8')
        else:
            self.user = user

        if isinstance(password, str):
            self.password = password.encode('utf8')
        else:
            self.password = password

        self.autocommit = True
            
        self._caches = {}
        self.commandNumber = -1
        self.status = None
        
        try:
            if unix_sock is None and host is not None:
                self._usock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            elif unix_sock is not None:
                if not hasattr(socket, "AF_UNIX"):
                    raise InterfaceError(
                        "attempt to connect to unix socket on unsupported "
                        "platform")
                self._usock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            else:
                raise ProgrammingError(
                    "one of host or unix_sock must be provided")
            if timeout is not None:
                self._usock.settimeout(timeout)

            if unix_sock is None and host is not None:
                self._usock.connect((host, port))
            elif unix_sock is not None:
                self._usock.connect(unix_sock)

            self._sock = self._usock.makefile(mode="rwb")
            if tcp_keepalive:
                self._usock.setsockopt(
                    socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except socket.error as e:
            self._usock.close()
            raise InterfaceError("communication error", e)
        self._flush = self._sock.flush
        self._read = self._sock.read
        self._write = self._sock.write
        self._backend_key_data = None

        def text_out(v):
            return v.encode(self._client_encoding)

        def enum_out(v):
            return str(v.value).encode(self._client_encoding)

        def time_out(v):
            return v.isoformat().encode(self._client_encoding)

        def date_out(v):
            return v.isoformat().encode(self._client_encoding)

        def unknown_out(v):
            return str(v).encode(self._client_encoding)

        trans_tab = dict(zip(map(ord, '{}'), '[]'))
        glbls = {'Decimal': Decimal}

        def array_in(data, idx, length):
            arr = []
            prev_c = None
            for c in data[idx:idx+length].decode(
                    self._client_encoding).translate(
                    trans_tab).replace('NULL', 'None'):
                if c not in ('[', ']', ',', 'N') and prev_c in ('[', ','):
                    arr.extend("Decimal('")
                elif c in (']', ',') and prev_c not in ('[', ']', ',', 'e'):
                    arr.extend("')")

                arr.append(c)
                prev_c = c
            return eval(''.join(arr), glbls)

        def array_recv(data, idx, length):
            final_idx = idx + length
            dim, hasnull, typeoid = iii_unpack(data, idx)
            idx += 12

            # get type conversion method for typeoid
            conversion = self.pg_types[typeoid][1]

            # Read dimension info
            dim_lengths = []
            for i in range(dim):
                dim_lengths.append(ii_unpack(data, idx)[0])
                idx += 8

            # Read all array values
            values = []
            while idx < final_idx:
                element_len, = i_unpack(data, idx)
                idx += 4
                if element_len == -1:
                    values.append(None)
                else:
                    values.append(conversion(data, idx, element_len))
                    idx += element_len

            # at this point, {{1,2,3},{4,5,6}}::int[][] looks like
            # [1,2,3,4,5,6]. go through the dimensions and fix up the array
            # contents to match expected dimensions
            for length in reversed(dim_lengths[1:]):
                values = list(map(list, zip(*[iter(values)] * length)))
            return values

        def vector_in(data, idx, length):
            return eval('[' + data[idx:idx+length].decode(
                self._client_encoding).replace(' ', ',') + ']')

        def text_recv(data, offset, length):
            return str(data[offset: offset + length], self._client_encoding)

        def bool_recv(data, offset, length):
            return data[offset] == 116  # ascii for t

        def json_in(data, offset, length):
            return loads(
                str(data[offset: offset + length], self._client_encoding))

        def time_in(data, offset, length):
            hour = int(data[offset:offset + 2])
            minute = int(data[offset + 3:offset + 5])
            sec = Decimal(
                data[offset + 6:offset + length].decode(self._client_encoding))
            return time(
                hour, minute, int(sec), int((sec - int(sec)) * 1000000))

        def date_in(data, offset, length):
            d = data[offset:offset+length].decode(self._client_encoding)
            try:
                return date(int(d[:4]), int(d[5:7]), int(d[8:10]))
            except ValueError:
                return d

        def numeric_in(data, offset, length):
            return Decimal(
                data[offset: offset + length].decode(self._client_encoding))

        def numeric_out(d):
            return str(d).encode(self._client_encoding)

        self.pg_types = defaultdict(
            lambda: (FC_TEXT, text_recv), {
                16: (FC_BINARY, bool_recv),  # boolean
                17: (FC_BINARY, bytea_recv),  # bytea
                19: (FC_BINARY, text_recv),  # name type
                20: (FC_BINARY, int8_recv),  # int8
                21: (FC_BINARY, int2_recv),  # int2
                22: (FC_TEXT, vector_in),  # int2vector
                23: (FC_BINARY, int4_recv),  # int4
                25: (FC_BINARY, text_recv),  # TEXT type
                26: (FC_TEXT, int_in),  # oid
                28: (FC_TEXT, int_in),  # xid
                114: (FC_TEXT, json_in),  # json
                700: (FC_BINARY, float4_recv),  # float4
                701: (FC_BINARY, float8_recv),  # float8
                705: (FC_BINARY, text_recv),  # unknown
                829: (FC_TEXT, text_recv),  # MACADDR type
                1000: (FC_BINARY, array_recv),  # BOOL[]
                1003: (FC_BINARY, array_recv),  # NAME[]
                1005: (FC_BINARY, array_recv),  # INT2[]
                1007: (FC_BINARY, array_recv),  # INT4[]
                1009: (FC_BINARY, array_recv),  # TEXT[]
                1014: (FC_BINARY, array_recv),  # CHAR[]
                1015: (FC_BINARY, array_recv),  # VARCHAR[]
                1016: (FC_BINARY, array_recv),  # INT8[]
                1021: (FC_BINARY, array_recv),  # FLOAT4[]
                1022: (FC_BINARY, array_recv),  # FLOAT8[]
                1042: (FC_BINARY, text_recv),  # CHAR type
                1043: (FC_BINARY, text_recv),  # VARCHAR type
                1082: (FC_TEXT, date_in),  # date
                1083: (FC_TEXT, time_in),
                1114: (FC_BINARY, timestamp_recv_float),  # timestamp w/ tz
                1184: (FC_BINARY, timestamptz_recv_float),
                1186: (FC_BINARY, interval_recv_integer),
                1231: (FC_TEXT, array_in),  # NUMERIC[]
                1263: (FC_BINARY, array_recv),  # cstring[]
                1700: (FC_TEXT, numeric_in),  # NUMERIC
                2275: (FC_BINARY, text_recv),  # cstring
                2950: (FC_BINARY, uuid_recv),  # uuid
                3802: (FC_TEXT, json_in),  # jsonb
            })

        self.py_types = {
            type(None): (-1, FC_BINARY, null_send),  # null
            bool: (16, FC_BINARY, bool_send),
            bytearray: (17, FC_BINARY, bytea_send),  # bytea
            20: (20, FC_BINARY, q_pack),  # int8
            21: (21, FC_BINARY, h_pack),  # int2
            23: (23, FC_BINARY, i_pack),  # int4
            PGText: (25, FC_TEXT, text_out),  # text
            float: (701, FC_BINARY, d_pack),  # float8
            PGEnum: (705, FC_TEXT, enum_out),
            date: (1082, FC_TEXT, date_out),  # date
            time: (1083, FC_TEXT, time_out),  # time
            1114: (1114, FC_BINARY, timestamp_send_integer),  # timestamp
            # timestamp w/ tz
            PGVarchar: (1043, FC_TEXT, text_out),  # varchar
            1184: (1184, FC_BINARY, timestamptz_send_integer),
            PGJson: (114, FC_TEXT, text_out),
            PGJsonb: (3802, FC_TEXT, text_out),
            Timedelta: (1186, FC_BINARY, interval_send_integer),
            Interval: (1186, FC_BINARY, interval_send_integer),
            Decimal: (1700, FC_TEXT, numeric_out),  # Decimal
            PGTsvector: (3614, FC_TEXT, text_out),
            UUID: (2950, FC_BINARY, uuid_send)}  # uuid

        self.inspect_funcs = {
            Datetime: self.inspect_datetime,
            list: self.array_inspect,
            tuple: self.array_inspect,
            int: self.inspect_int}

        self.py_types[bytes] = (17, FC_BINARY, bytea_send)  # bytea
        self.py_types[str] = (705, FC_TEXT, text_out)  # unknown
        self.py_types[enum.Enum] = (705, FC_TEXT, enum_out)

        def inet_out(v):
            return str(v).encode(self._client_encoding)

        def inet_in(data, offset, length):
            inet_str = data[offset: offset + length].decode(
                self._client_encoding)
            if '/' in inet_str:
                return ip_network(inet_str, False)
            else:
                return ip_address(inet_str)

        self.py_types[IPv4Address] = (869, FC_TEXT, inet_out)  # inet
        self.py_types[IPv6Address] = (869, FC_TEXT, inet_out)  # inet
        self.py_types[IPv4Network] = (869, FC_TEXT, inet_out)  # inet
        self.py_types[IPv6Network] = (869, FC_TEXT, inet_out)  # inet
        self.pg_types[869] = (FC_TEXT, inet_in)  # inet

        def conn_send_query():
        
            if not self.execute(self._cursor, "set nz_encoding to 'utf8'", None):
                return False
        
            #Set the Datestyle to the format the driver expects it to be in */
            if datestyle == 'MDY':
                query = "set DateStyle to 'US'"
            elif datestyle == 'DMY':
                query = "set DateStyle to 'EUROPEAN'"
            else:
                query = "set DateStyle to 'ISO'"  
            
            if not self.execute(self._cursor, query, None):
                return False           
           
            client_info = "select version(), 'Netezza Python Client Version: {}', '{}', 'OS Platform: {}', 'OS Username: {}'"
            
            query = client_info.format(nzpy_client_version,platform.uname().machine,platform.system(),getpass.getuser())
            
            if not self.execute(self._cursor, query, None):
                return False
            else: 
                results = self._cursor.fetchall()
                for c1, c2, c3, c4, c5 in results:
                    logging.debug("c1 = %s, c2 = %s, c3 = %s, c4 = %s, c5 = %s" % (c1,c2,c3,c4,c5))
                
            client_info = "SET CLIENT_VERSION = '{}'"
            query = client_info.format(nzpy_client_version)
            if not self.execute(self._cursor, query, None):
                return False
                
            if not self.execute(self._cursor, "select ascii(' ') as space, encoding as ccsid from _v_database where objid = current_db", None):
                return False
            else: 
                results = self._cursor.fetchall()
                for c1, c2 in results:
                    logging.debug("c1 = %s, c2 = %s" % (c1,c2))
                    
            if not self.execute(self._cursor, "select feature from _v_odbc_feature where spec_level = '3.5'", None):
                return False
            else: 
                results = self._cursor.fetchall()
                for c1 in results:
                    logging.debug("c1 = %s" % (c1))
            
            if not self.execute(self._cursor, "select identifier_case, current_catalog, current_user", None):
                return False
            else: 
                results = self._cursor.fetchall()
                for c1, c2, c3 in results:
                    logging.debug("c1 = %s, c2 = %s, c3 = %s" % (c1,c2,c3))
                
            return True
   
        self.message_types = {
            NOTICE_RESPONSE: self.handle_NOTICE_RESPONSE,
            PARAMETER_STATUS: self.handle_PARAMETER_STATUS,
            READY_FOR_QUERY: self.handle_READY_FOR_QUERY,
            ROW_DESCRIPTION: self.handle_ROW_DESCRIPTION,
            ERROR_RESPONSE: self.handle_ERROR_RESPONSE,
            EMPTY_QUERY_RESPONSE: self.handle_EMPTY_QUERY_RESPONSE,
            DATA_ROW: self.handle_DATA_ROW,
            COMMAND_COMPLETE: self.handle_COMMAND_COMPLETE,
            PARSE_COMPLETE: self.handle_PARSE_COMPLETE,
            BIND_COMPLETE: self.handle_BIND_COMPLETE,
            CLOSE_COMPLETE: self.handle_CLOSE_COMPLETE,
            PORTAL_SUSPENDED: self.handle_PORTAL_SUSPENDED,
            NO_DATA: self.handle_NO_DATA,
            PARAMETER_DESCRIPTION: self.handle_PARAMETER_DESCRIPTION,
            NOTIFICATION_RESPONSE: self.handle_NOTIFICATION_RESPONSE,
            COPY_DONE: self.handle_COPY_DONE,
            COPY_DATA: self.handle_COPY_DATA,
            COPY_IN_RESPONSE: self.handle_COPY_IN_RESPONSE,
            COPY_OUT_RESPONSE: self.handle_COPY_OUT_RESPONSE}
        
        hs = handshake.Handshake(self._usock, self._sock, ssl)
        response = hs.startup(database, securityLevel, user, password)
        
        if response is not False: 
            self._flush = response.flush
            self._read = response.read
            self._write = response.write        
        else: 
            raise ProgrammingError("Error in handshake")
        
        self._cursor = self.cursor()
        code = self.error = None
        
        if not conn_send_query():
            logging.warning("Error sending initial setup queries")
            
        self.commandNumber = 0
                
        if self.error is not None:
            raise ProgrammingError(self.error)    

        self.in_transaction = False

    def handle_ERROR_RESPONSE(self, data, ps):
        msg = dict(
            (
                s[:1].decode(self._client_encoding),
                s[1:].decode(self._client_encoding)) for s in
            data.split(NULL_BYTE) if s != b'')

        response_code = msg[RESPONSE_CODE]
        if response_code == '28000':
            cls = InterfaceError
        elif response_code == '23505':
            cls = IntegrityError
        else:
            cls = ProgrammingError

        self.error = cls(msg)

    def handle_EMPTY_QUERY_RESPONSE(self, data, ps):
        self.error = ProgrammingError("query was empty")

    def handle_CLOSE_COMPLETE(self, data, ps):
        pass

    def handle_PARSE_COMPLETE(self, data, ps):
        # Byte1('1') - Identifier.
        # Int32(4) - Message length, including self.
        pass

    def handle_BIND_COMPLETE(self, data, ps):
        pass

    def handle_PORTAL_SUSPENDED(self, data, cursor):
        pass

    def handle_PARAMETER_DESCRIPTION(self, data, ps):
        # Well, we don't really care -- we're going to send whatever we
        # want and let the database deal with it.  But thanks anyways!

        # count = h_unpack(data)[0]
        # type_oids = unpack_from("!" + "i" * count, data, 2)
        pass

    def handle_COPY_DONE(self, data, ps):
        self._copy_done = True

    def handle_COPY_OUT_RESPONSE(self, data, ps):
        # Int8(1) - 0 textual, 1 binary
        # Int16(2) - Number of columns
        # Int16(N) - Format codes for each column (0 text, 1 binary)

        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        if ps.stream is None:
            raise InterfaceError(
                "An output stream is required for the COPY OUT response.")

    def handle_COPY_DATA(self, data, ps):
        ps.stream.write(data)

    def handle_COPY_IN_RESPONSE(self, data, ps):
        # Int16(2) - Number of columns
        # Int16(N) - Format codes for each column (0 text, 1 binary)
        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)
        if ps.stream is None:
            raise InterfaceError(
                "An input stream is required for the COPY IN response.")

        bffr = bytearray(8192)
        while True:
            bytes_read = ps.stream.readinto(bffr)
            if bytes_read == 0:
                break
            self._write(COPY_DATA + i_pack(bytes_read + 4))
            self._write(bffr[:bytes_read])
            self._flush()

        # Send CopyDone
        # Byte1('c') - Identifier.
        # Int32(4) - Message length, including self.
        self._write(COPY_DONE_MSG)
        self._write(SYNC_MSG)
        self._flush()

    def handle_NOTIFICATION_RESPONSE(self, data, ps):
        ##
        # A message sent if this connection receives a NOTIFY that it was
        # LISTENing for.
        # <p>
        # Stability: Added in nzpy v1.03.  When limited to accessing
        # properties from a notification event dispatch, stability is
        # guaranteed for v1.xx.
        backend_pid = i_unpack(data)[0]
        idx = 4
        null = data.find(NULL_BYTE, idx) - idx
        condition = data[idx:idx + null].decode("ascii")
        idx += null + 1
        null = data.find(NULL_BYTE, idx) - idx
        # additional_info = data[idx:idx + null]

        self.notifications.append((backend_pid, condition))

    def cursor(self):
        """Creates a :class:`Cursor` object bound to this
        connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        return Cursor(self)

    def commit(self):
        """Commits the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        self.execute(self._cursor, "commit", None)

    def rollback(self):
        """Rolls back the current database transaction.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        if not self.in_transaction:
            return
        self.execute(self._cursor, "rollback", None)

    def close(self):
        """Closes the database connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """
        try:
            # Byte1('X') - Identifies the message as a terminate message.
            # Int32(4) - Message length, including self.
            self._write(TERMINATE_MSG)
            self._flush()
            self._sock.close()
        except AttributeError:
            raise InterfaceError("connection is closed")
        except ValueError:
            raise InterfaceError("connection is closed")
        except socket.error:
            pass
        finally:
            self._usock.close()
            self._sock = None

    def handle_READY_FOR_QUERY(self, data, ps):
        # Byte1 -   Status indicator.
        self.in_transaction = data != IDLE

    def inspect_datetime(self, value):
        if value.tzinfo is None:
            return self.py_types[1114]  # timestamp
        else:
            return self.py_types[1184]  # send as timestamptz

    def inspect_int(self, value):
        if min_int2 < value < max_int2:
            return self.py_types[21]
        if min_int4 < value < max_int4:
            return self.py_types[23]
        if min_int8 < value < max_int8:
            return self.py_types[20]

    def make_params(self, values):
        params = []
        for value in values:
            typ = type(value)
            try:
                params.append(self.py_types[typ])
            except KeyError:
                try:
                    params.append(self.inspect_funcs[typ](value))
                except KeyError as e:
                    param = None
                    for k, v in self.py_types.items():
                        try:
                            if isinstance(value, k):
                                param = v
                                break
                        except TypeError:
                            pass

                    if param is None:
                        for k, v in self.inspect_funcs.items():
                            try:
                                if isinstance(value, k):
                                    param = v(value)
                                    break
                            except TypeError:
                                pass
                            except KeyError:
                                pass

                    if param is None:
                        raise NotSupportedError(
                            "type " + str(e) + " not mapped to pg type")
                    else:
                        params.append(param)

        return tuple(params)

    def handle_ROW_DESCRIPTION(self, data, cursor):
        count = h_unpack(data)[0]
        idx = 2
        for i in range(count):
            name = data[idx:data.find(NULL_BYTE, idx)]
            idx += len(name) + 1
            field = dict(
                zip((
                    "type_oid", "type_size","type_modifier","format"), ihic_unpack(data, idx)))
            field['name'] = name
            idx += 11
            cursor.ps['row_desc'].append(field)
            field['nzpy_fc'], field['func'] = \
                self.pg_types[field['type_oid']]
    
    def Prepare(self, cursor, query, vals):    
        
        statement, make_args = convert_paramstyle(nzpy.paramstyle, query)
        args = make_args(vals)
        placeholderCount = query.count('?')
        if len(args) >= 65536 :
                logging.warning("got %d parameters but PostgreSQL only supports 65535 parameters", len(args))
        if len(args) != placeholderCount :
                logging.warning("got %d parameters but the statement requires %d", len(args), placeholderCount)
	
        for arg in args:
            if isinstance(arg, str):
                strfmt = "'{}'"                
                query = query.replace('?', strfmt.format(arg), 1)
            elif isinstance(arg, bytes):
                bytfmt = "x'{}'"                
                query = query.replace('?', bytfmt.format(arg.decode(self._client_encoding)), 1)                
            else:
                query = query.replace('?', str(arg), 1)
        
        if statement.find('select') != -1 or statement.find('SELECT') != -1 :
            statement = statement + " ANALYZE"
            self.execute(cursor, statement, None)
                        
        return query            
        
    def execute(self, cursor, query, vals):
        
        cursor._row_count = -1
        cursor.ps = {'row_desc': []}

        if vals is None:
            vals = ()
        else:    
            query = self.Prepare(cursor, query, vals)
            
        if self.status == CONN_EXECUTING:
            self._read(4)
            
        buf = bytearray( b'P\xFF\xFF\xFF\xFF')
        
        if self.commandNumber != -1 :
            self.commandNumber += 1
            buf = bytearray(b'P' + i_pack(self.commandNumber))
            
        if self.commandNumber > 100000:
            self.commandNumber = 1
        
        if query is not None:
            if isinstance(query, str):
                query = query.encode('utf8')
        buf.extend(query + NULL_BYTE)
        self._write(buf)
        self._flush()
        
        logging.debug("Buffer sent to nps:%s", buf)
        
        self.status = CONN_EXECUTING
        
        response = self.connNextResultSet(cursor)

        if self.error is not None:
            raise ProgrammingError(self.error)    
            
        return response
        
    def connNextResultSet(self, cursor):
            
        fname = None
        fh = None
        
        while(1):
            response = self._read(1)
            logging.debug("Backend response: %s",response)
            self._read(4)
            
            if response == COMMAND_COMPLETE:  # portal query command, no tuples returned 
                length = i_unpack(self._read(4))[0]
                data = self._read(length)
                self.handle_COMMAND_COMPLETE(data,cursor)
                logging.debug ("Response received from backend: %s", str(data,self._client_encoding))
                continue
            if response == READY_FOR_QUERY:
                return True
            if response == b"L":
                return True
            if response == b"0":
                pass
            if response == b"A":
                pass
            if response == b"P":
                length = i_unpack(self._read(4))[0]
                logging.debug ("Response received from backend:%s", str(self._read(length),self._client_encoding))
                continue
            if response == ERROR_RESPONSE:
                length = i_unpack(self._read(4))[0]
                self.error = str(self._read(length),self._client_encoding)
                logging.debug ("Response received from backend:%s", self.error)
                return False
            if response == ROW_DESCRIPTION:
                length = i_unpack(self._read(4))[0]
                cursor.ps = {'row_desc': []}
                self.handle_ROW_DESCRIPTION(self._read(length),cursor)
                # We've got row_desc that allows us to identify what we're
                # going to get back from this statement.
                cursor.ps['input_funcs'] = tuple(f['func'] for f in cursor.ps['row_desc'])
            if response == DATA_ROW:
                length = i_unpack(self._read(4))[0]
                self.handle_DATA_ROW(self._read(length), cursor)
            if response == b"X":
                length = i_unpack(self._read(4))[0]
                self.tupdesc = DbosTupleDesc()
                self.Res_get_dbos_column_descriptions(self._read(length),self.tupdesc)
                continue
            if response == b"Y":
                self.Res_read_dbos_tuple(cursor, self.tupdesc)
                continue
            if response == b"u": #unload - initialize application protocol
                # in ODBC, the first 10 bytes are utilized to populate clientVersion, formatType and bufSize
                # these are not needed in go lang, hence ignoring 10 bytes
                self._read(10)
                # Next 16 bytes are Reserved Bytes for future extension
                self._read(16)
                # Get the filename (specified in dataobject)
                length = i_unpack(self._read(4))[0]
                fnameBuf = self._read(length)
                fname = str(fnameBuf,self._client_encoding)
                try:
                    fh = open(fname, "w+")
                    logging.debug("Successfully opened file: %s", fname)
                    #file open successfully, send status back to datawriter                
                    buf = bytearray(i_pack(0))
                    self._write(buf)
                    self._flush()     
                except:
                    logging.warning("Error while opening file")
            
            if response == b"U": # handle unload data 
                self.receiveAndWriteDatatoExternal(fname, fh)
                
            if response == b"l": 
                self.xferTable()
                
            if response == b"x": # handle Ext Tbl parser abort 
                self._read(4)
                logging.warning("Error operation cancel")
            
            if response == b"e":
            
                length = i_unpack(self._read(4))[0]
                logDir = str(self._read(length-1),self._client_encoding)
                
                self._read(1)  # ignore one byte as it is null character at the end of the string
                char = c_unpack(self._read(1))[0]
                filenameBuf = bytearray(char)
                while True :            
                    char = c_unpack(self._read(1))[0]
                    if char == b'\x00':
                        break
                    filenameBuf.extend(char)
        
                filename = str(filenameBuf,self._client_encoding)
                logType = i_unpack(self._read(4))[0]
                if not self.getFileFromBE(logDir, filename, logType):
                    logging.debug("Error in writing file received from BE")
                continue
            
            if response == NOTICE_RESPONSE:            
                length = i_unpack(self._read(4))[0]
                self.notices = str(self._read(length),self._client_encoding)
                logging.debug ("Response received from backend:%s", self.notices)                              
            
            if response == b"I":         
                length = i_unpack(self._read(4))[0]
                self.notices = str(self._read(length),self._client_encoding)
                logging.debug ("Response received from backend:%s", self.notices)                
                cursor._cached_rows.append([])
     
    def Res_get_dbos_column_descriptions(self, data, tupdesc):
        
        data_idx = 0 
        tupdesc.version = i_unpack(data, data_idx)[0]         
        tupdesc.nullsAllowed = i_unpack(data, data_idx+4)[0]      
        tupdesc.sizeWord = i_unpack(data, data_idx+8)[0]          
        tupdesc.sizeWordSize = i_unpack(data, data_idx+12)[0]      
        tupdesc.numFixedFields = i_unpack(data, data_idx+16)[0]    
        tupdesc.numVaryingFields = i_unpack(data, data_idx+20)[0]  
        tupdesc.fixedFieldsSize = i_unpack(data, data_idx+24)[0]   
        tupdesc.maxRecordSize = i_unpack(data, data_idx+28)[0]     
        tupdesc.numFields = i_unpack(data, data_idx+32)[0] 

        data_idx += 36
        for ix in range(tupdesc.numFields):
            tupdesc.field_type.append(i_unpack(data, data_idx)[0])
            tupdesc.field_size.append(i_unpack(data, data_idx+4)[0])     
            tupdesc.field_trueSize.append(i_unpack(data, data_idx+8)[0])   
            tupdesc.field_offset.append(i_unpack(data, data_idx+12)[0])    
            tupdesc.field_physField.append(i_unpack(data, data_idx+16)[0])   
            tupdesc.field_logField.append(i_unpack(data, data_idx+20)[0])  
            tupdesc.field_nullAllowed.append(i_unpack(data, data_idx+24)[0])
            tupdesc.field_fixedSize.append(i_unpack(data, data_idx+28)[0])  
            tupdesc.field_springField.append(i_unpack(data, data_idx+32)[0])
            data_idx += 36
        
        tupdesc.DateStyle = i_unpack(data, data_idx)[0]         
        tupdesc.EuroDates = i_unpack(data, data_idx+4)[0]                                         
        
    def Res_read_dbos_tuple(self, cursor, tupdesc): 
        
        numFields = tupdesc.numFields
        
        length = i_unpack(self._read(4))[0]
        logging.debug("Length of the message from backend:%s", length)
        length = i_unpack(self._read(4))[0]
        logging.debug("Length of the message from backend:%s", length)
        data = self._read(length)
        logging.debug("Actual message is: %s", data)
        
        if length > tupdesc.maxRecordSize :
            maxRecordSize = length
            
        # bitmaplen denotes the number of bytes bitmap sent by backend. For e.g.: for select 
        # statement with 9 columns, we would receive 2 bytes bitmap.
        bitmaplen = numFields // 8                
        if (numFields % 8) > 0 :
            bitmaplen+=1        
            
        # We ignore first 2 bytes as that denotes length of message. Now convert hex to dec 
        hex = data[2:2+bitmaplen].hex()
        dec = int(hex, 16)
        
        # convert dec to binary
        bitmap = decimalToBinary(dec,bitmaplen*8)
               
        field_lf = 0
        cur_field = 0
        row = []
        
        while field_lf < numFields and cur_field < numFields :
            
            fieldDataP = self.CTable_FieldAt(tupdesc, data, cur_field)
            
            # a bitmap with value of 1 denotes null column
            if bitmap[tupdesc.field_physField[field_lf]] == 1 and tupdesc.nullsAllowed != 0 :
                row.append(None)
                logging.debug("field=%d, value= NULL", cur_field+1)
                cur_field += 1
                field_lf +=1
                continue
		
            # Fldlen is byte-length of backend-datatype
            # memsize is byte-length of ODBC-datatype or internal-datatype for (Numeric/Interval)
            fldlen = self.CTable_i_fieldSize(tupdesc, cur_field)
            memsize = fldlen
            fldtype = self.CTable_i_fieldType(tupdesc, cur_field)
            
            if fldtype == NzTypeUnknown: 
                fldtype = NzTypeVarChar
                memsize = memsize + 1
            if fldtype == NzTypeChar or fldtype == NzTypeVarChar or fldtype == NzTypeVarFixedChar or fldtype == NzTypeGeometry or fldtype == NzTypeVarBinary:
                memsize = memsize + 1
            if fldtype == NzTypeNChar or fldtype == NzTypeNVarChar: 
                memsize *= 4
                memsize = memsize + 1 
            if fldtype == NzTypeDate:
                memsize = 12
            if fldtype == NzTypeTime:
                memsize = 8
            if fldtype == NzTypeInterval:
                memsize = 12
            if fldtype == NzTypeTimeTz:
                memsize = 15
            if fldtype == NzTypeTimestamp:
                memsize = 8
            if fldtype == NzTypeBool:
                memsize = 1
                
            if fldtype == NzTypeChar:
                value = str(fieldDataP[:fldlen], self._client_encoding)
                row.append(value)
                logging.debug("field=%d, datatype=CHAR, value=%s", cur_field+1,value)                
                
            if fldtype == NzTypeNChar or fldtype == NzTypeVarFixedChar:
                cursize  = int.from_bytes(fieldDataP[0:2], 'little') - 2
                value = str(fieldDataP[2:cursize+2], self._client_encoding)
                row.append(value)
                logging.debug("field=%d, datatype=%s, value=%s", cur_field+1,dataType[fldtype], value)                
                
            if fldtype == NzTypeVarChar or fldtype == NzTypeNVarChar or fldtype == NzTypeGeometry or fldtype == NzTypeVarBinary:
                cursize  = int.from_bytes(fieldDataP[0:2], 'little') - 2
                value = str(fieldDataP[2:cursize+2], self._client_encoding)
                row.append(value)
                logging.debug("field=%d, datatype=%s, value=%s", cur_field+1,dataType[fldtype], value)
                
            if fldtype == NzTypeInt8:  #int64
                value = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                row.append(value)
                logging.debug("field=%d, datatype=NzTypeInt8, value=%s", cur_field+1, value)
            
            if fldtype == NzTypeInt:   #int32
                value = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                row.append(value)
                logging.debug("field=%d, datatype=NzTypeInt4, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeInt2:  #int16
                value = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                row.append(value)
                logging.debug("field=%d, datatype=NzTypeInt2, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeInt1:  #int8
                value = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                row.append(value)
                logging.debug("field=%d, datatype=NzTypeInt1, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeDouble: 
                value = struct.unpack('d', fieldDataP[:fldlen])[0]
                row.append(value)
                logging.debug("field=%d, datatype=NzTypeDouble, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeFloat:
                value = struct.unpack('f', fieldDataP[:fldlen])[0]
                row.append(value)
                logging.debug("field=%d, datatype=NzTypeFloat, value=%s", cur_field+1, value)                
            
            if fldtype == NzTypeDate:                   
                workspace = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                date_value = j2date(workspace + date2j(2000, 1, 1))                
                date_format = "{0:02d}-{1:02d}-{2:02d}"
                value = date_format.format(date_value[0],date_value[1],date_value[2])
                row.append(value)
                logging.debug("field=%d, datatype=DATE, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeTime:
                workspace = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                time_value = time2struct(workspace)
                time_format = "{0:02d}:{1:02d}:{2:02d}"
                value = time_format.format(time_value[0],time_value[1],time_value[2])
                row.append(value)
                logging.debug("field=%d, datatype=TIME, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeInterval:
                interval_time = int.from_bytes(fieldDataP[:fldlen-4], byteorder='little', signed=True)
                interval_month = int.from_bytes(fieldDataP[fldlen-4:fldlen], byteorder='little', signed=True)
                value = IntervalToText(interval_time,interval_month)
                row.append(value)
                logging.debug("field=%d, datatype=INTERVAL, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeTimeTz:
                timetz_time = int.from_bytes(fieldDataP[:fldlen-4], byteorder='little', signed=True)                
                timetz_zone = int.from_bytes(fieldDataP[fldlen-4:fldlen], byteorder='little', signed=True)                
                value = timetz_out_timetzadt(timetz_time, timetz_zone)
                row.append(value)
                logging.debug("field=%d, datatype=TIMETZ, value=%s", cur_field+1, value)
                
            if fldtype == NzTypeTimestamp:
                if fldlen == 8 :
                    workspace = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                elif fldlen == 4:
                    workspace = int.from_bytes(fieldDataP[:fldlen], byteorder='little', signed=True)
                
                if fldlen == 8 :
                    timestamp_value = timestamp2struct(workspace)
                elif fldlen == 4 :
					#could not find any case for the same and hence not implemented yet
					#abstime2struct(workspace, &timestamp_value)
                    pass
                
                time_format = "{0:02d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}.{6:02d}"
                value = time_format.format(timestamp_value[0],timestamp_value[1],timestamp_value[2],timestamp_value[3],timestamp_value[4],timestamp_value[5],timestamp_value[6])
                row.append(value)
                logging.debug("field=%d, datatype=TIMESTAMP, value=%s", cur_field+1, value)            
            
            if fldtype == NzTypeNumeric:
                
                prec = self.CTable_i_fieldPrecision(tupdesc, cur_field)
                scale = self.CTable_i_fieldScale(tupdesc, cur_field)
                count = self.CTable_i_fieldNumericDigit32Count(tupdesc, cur_field)
                
                if prec <= 9 :
                    num_parts = 1
                elif prec <= 18 :
                    num_parts = 2
                else:
                    num_parts = 4
                
                dataBuffer = []

                if numeric.NDIGIT_INT64 :
                    for i in range(num_parts) :
                        dataBuffer.append(int.from_bytes(fieldDataP[:8], byteorder='little', signed=False))
                        fieldDataP = fieldDataP[8:]                    
                else :
                    for i in range(num_parts) :
                        dataBuffer.append(int.from_bytes(fieldDataP[:4], byteorder='little', signed=False))
                        fieldDataP = fieldDataP[4:]              
          
                buffer = numeric.PYTHON_numeric_load_var(dataBuffer, prec, scale, count)
                value = numeric.get_str_from_var(buffer, buffer.rscale)
                row.append(value)
                logging.debug("field=%d, datatype=NUMERIC, value=%s", cur_field+1, value) 
                
            if fldtype == NzTypeBool:                
                value = fieldDataP[:fldlen] == b'\x01'
                row.append(value)
                logging.debug("field=%d, datatype=BOOL, value=%s", cur_field+1, value)                
           
            cur_field += 1
            field_lf += 1
            
        cursor._cached_rows.append(row)
    
    def CTable_FieldAt(self, tupdesc, data, cur_field): 
        if tupdesc.field_fixedSize[cur_field] != 0 :
            return self.CTable_i_fixedFieldPtr(data, tupdesc.field_offset[cur_field])
        
        return self.CTable_i_varFieldPtr(data, tupdesc.fixedFieldsSize, tupdesc.field_offset[cur_field])

    def CTable_i_fixedFieldPtr(self, data, offset):
        data = data[offset:]
        return data

    def CTable_i_varFieldPtr(self, data, fixedOffset, varDex): 

        lenP = data[fixedOffset:]
        for ctr in range(varDex) :
            length = int.from_bytes(lenP[0:2], 'little') 
            if length%2 == 0 :
                lenP = lenP[length:]
            else :
                lenP = lenP[length+1:]            
        
        return lenP        
    
    def CTable_i_fieldType(self, tupdesc , coldex) :
        return (tupdesc.field_type[coldex])

    def CTable_i_fieldSize(self, tupdesc , coldex) :
        return (tupdesc.field_size[coldex])

    def CTable_i_fieldPrecision(self, tupdesc, coldex):
        return (((tupdesc.field_size[coldex]) >> 8) & 0x7F)

    def CTable_i_fieldScale(self, tupdesc, coldex):
        return ((tupdesc.field_size[coldex]) & 0x00FF)

    def CTable_i_fieldNumericDigit32Count(self, tupdesc, coldex):
        sizeTNumericDigit = 4
        return int(tupdesc.field_trueSize[coldex] / sizeTNumericDigit) #sizeof(TNumericDigit)
    
    def receiveAndWriteDatatoExternal(self, fname, fh):
        
        self._read(4)
    
        while(1) :
		
            #  Get EXTAB_SOCK Status
            try: 
                status = i_unpack(self._read(4))[0]
            except:
                logging.warning("Error while retrieving status, closing unload file")
            finally:
                fh.close()     
		
            if status == EXTAB_SOCK_DATA:
                # get number of bytes in block
                numBytes = i_unpack(self._read(4))[0]
                try:                    
                    blockBuffer = str(self._read(numBytes),self._client_encoding) 
                    fh = open(fname, "w+")
                    fh.write(blockBuffer)
                    logging.info("Successfully written data into file")
                except: 
                    logging.warning("Error in writing data to file")
                continue

            if status ==  EXTAB_SOCK_DONE:
                fh.close()
                logging.info("unload - done receiving data")
                break

            if status == EXTAB_SOCK_ERROR:
    
                len = h_unpack(self._read(2))[0]
                errorMsg = str(self._read(length),self._client_encoding)

                len = h_unpack(self._read(2))[0]
                errorObject = str(self._read(length),self._client_encoding)

                logging.warning("unload - ErrorMsg: %s", errorMsg)
                logging.warning("unload - ErrorObj: %s", errorObject)

                fh.close()
                logging.debug("unload - done receiving data")
                return

            else:
                fh.close()
                return
		
        return 	
    
    def xferTable(self):

        self._read(4)
        clientversion = 1
        
        char = c_unpack(self._read(1))[0]
        filenameBuf = bytearray(char)
        while True :            
            char = c_unpack(self._read(1))[0]
            if char == b'\x00':
                break
            filenameBuf.extend(char)
        
        filename = str(filenameBuf,self._client_encoding)
        
        hostversion = i_unpack(self._read(4))[0]
        
        val = bytearray( i_pack(clientversion))
        self._write(val)
        self._flush()

        format = i_unpack(self._read(4))[0]
        blockSize = i_unpack(self._read(4))[0]
        byteread = blockSize
        logging.info("Format=%d Block size=%d Host version=%d ", format, blockSize, hostversion)

        try: 
            filehandle = open(filename,'r')
            logging.info("Successfully opened External file to read:%s", filename)            
            while True :                
                data = filehandle.read(blockSize)
                if not data:
                    break
                if blockSize < len(data.encode('utf8')):
                    diff = len(data.encode('utf8')) - blockSize
                    val = bytearray(i_pack(EXTAB_SOCK_DATA)+ i_pack(blockSize))
                    val.extend(data.encode('utf8'))
                    self._write(val[:blockSize+8])
                    self._flush()
                    val = bytearray(i_pack(EXTAB_SOCK_DATA)+ i_pack(diff)+ val[blockSize+8:])                    
                    self._write(val)
                    self._flush()
                else:
                    val = bytearray(i_pack(EXTAB_SOCK_DATA)+ i_pack(len(data.encode('utf8'))))
                    val.extend(data.encode('utf8'))
                    self._write(val)
                    self._flush()
                logging.debug("No. of bytes sent to BE:%s", len(data))                
            val = bytearray(i_pack(EXTAB_SOCK_DONE))  
            self._write(val)
            self._flush() 
            logging.info("sent EXTAB_SOCK_DONE to reader")
                
        except:
            logging.warning("Error opening file")
	
 #################################################################################
 # Function: getFileFromBE - This Routine opens a file in the temp directory
 #           using the filename specified by the BE in /tmp or c:\.
 #           The data sent by the BE are then written into this file.
 #
 # Parameters:
 #
 #  In       logDir - directory to put the file
 #           filename - name of file to write.
 #           logType - not used at this implementation.
 #
 #  Out      boolean - success or failure.
 #
 #################################################################################
    def getFileFromBE(self, logDir, filename, logType):
        
        status = True
        
        #If no explicit -logDir mentioned (defaulted by backend to /tmp)
        if platform.system() == "Windows" :
            fullpath = logDir + "\\" + filename
        elif platform.system() == "Linux" :
            fullpath = logDir + "/" + filename        
    
        if logType == 1 :
            fullpath = fullpath + ".nzlog"
            fh = open(fullpath, "w+")
        elif logType == 2 :
            fullpath = fullpath + ".nzbad"
            fh = open(fullpath, "w+")
        elif logType == 3 :
            fullpath = fullpath + ".nzstats"
            fh = open(fullpath, "w+")
        
        while(1):

            numBytes = i_unpack(self._read(4))[0]

            if numBytes == 0 : #zeros means EOF, no more data
                break           

            dataBuffer = str(self._read(numBytes),self._client_encoding) 

            if status :
                try:
                    fh.write(dataBuffer)
                    logging.info("Successfully written data into file: %s", fullpath)	                    
                except: 
                    logging.warning("Error in writing data to file")
                    status = False

        fh.close()	
        return status
        
    def _send_message(self, code, data):
        try:
            self._write(code)
            self._write(i_pack(len(data) + 4))
            self._write(data)
            self._write(FLUSH_MSG)
        except ValueError as e:
            if str(e) == "write to closed file":
                raise InterfaceError("connection is closed")
            else:
                raise e
        except AttributeError:
            raise InterfaceError("connection is closed")

    def send_EXECUTE(self, cursor):
        # Byte1('E') - Identifies the message as an execute message.
        # Int32 -   Message length, including self.
        # String -  The name of the portal to execute.
        # Int32 -   Maximum number of rows to return, if portal
        #           contains a query # that returns rows.
        #           0 = no limit.
        self._write(EXECUTE_MSG)
        self._write(FLUSH_MSG)

    def handle_NO_DATA(self, msg, ps):
        pass
        
    def handle_COMMAND_COMPLETE(self, data, cursor):
        values = data[:-1].split(b' ')
        command = values[0]
        if command in self._commands_with_count:
            row_count = int(values[-1])
            if cursor._row_count == -1:
                cursor._row_count = row_count
            else:
                cursor._row_count += row_count

        if command in (b"ALTER", b"CREATE"):
            for scache in self._caches.values():
                for pcache in scache.values():
                    for ps in pcache['ps'].values():
                        self.close_prepared_statement(ps['statement_name_bin'])
                    pcache['ps'].clear()

    def handle_DATA_ROW(self, data, cursor):
        
        # bitmaplen denotes the number of bytes bitmap sent by backend. For e.g.: for select 
        # statement with 9 columns, we would receive 2 bytes bitmap.
        numberofcol = len(cursor.ps['row_desc']) 
        bitmaplen = numberofcol // 8                
        if (numberofcol % 8) > 0 :
            bitmaplen+=1        
            
        # convert hex to dec 
        hex = data[0:bitmaplen].hex()
        dec = int(hex, 16)
        
        # convert dec to binary
        bitmap = decimalToBinary(dec,bitmaplen*8)
        bitmap.reverse()
        
        data_idx = bitmaplen
        row = []
        for i, func in enumerate(cursor.ps['input_funcs']):
            if  bitmap[i] == 0:
                row.append(None)            
            else:
                vlen = i_unpack(data, data_idx)[0]
                data_idx += 4
                row.append(func(data, data_idx, vlen-4))
                data_idx += vlen-4
            
        cursor._cached_rows.append(row)

    def handle_messages(self, cursor):
        code = self.error = None

        while code != READY_FOR_QUERY:
            code, data_len = ci_unpack(self._read(5))
            self.message_types[code](self._read(data_len - 4), cursor)

        if self.error is not None:
            raise self.error

    # Byte1('C') - Identifies the message as a close command.
    # Int32 - Message length, including self.
    # Byte1 - 'S' for prepared statement, 'P' for portal.
    # String - The name of the item to close.
    def close_prepared_statement(self, statement_name_bin):
        self._send_message(CLOSE, STATEMENT + statement_name_bin)
        self._write(SYNC_MSG)
        self._flush()
        self.handle_messages(self._cursor)

    # Byte1('N') - Identifier
    # Int32 - Message length
    # Any number of these, followed by a zero byte:
    #   Byte1 - code identifying the field type (see responseKeys)
    #   String - field value
    def handle_NOTICE_RESPONSE(self, data, ps):
        self.notices.append(
            dict((s[0:1], s[1:]) for s in data.split(NULL_BYTE)))

    def handle_PARAMETER_STATUS(self, data, ps):
        pos = data.find(NULL_BYTE)
        key, value = data[:pos], data[pos + 1:-1]
        self.parameter_statuses.append((key, value))
        if key == b"client_encoding":
            encoding = value.decode("ascii").lower()
            self._client_encoding = pg_to_py_encodings.get(encoding, encoding)

        elif key == b"integer_datetimes":
            if value == b'on':

                self.py_types[1114] = (1114, FC_BINARY, timestamp_send_integer)
                self.pg_types[1114] = (FC_BINARY, timestamp_recv_integer)

                self.py_types[1184] = (
                    1184, FC_BINARY, timestamptz_send_integer)
                self.pg_types[1184] = (FC_BINARY, timestamptz_recv_integer)

                self.py_types[Interval] = (
                    1186, FC_BINARY, interval_send_integer)
                self.py_types[Timedelta] = (
                    1186, FC_BINARY, interval_send_integer)
                self.pg_types[1186] = (FC_BINARY, interval_recv_integer)
            else:
                self.py_types[1114] = (1114, FC_BINARY, timestamp_send_float)
                self.pg_types[1114] = (FC_BINARY, timestamp_recv_float)
                self.py_types[1184] = (1184, FC_BINARY, timestamptz_send_float)
                self.pg_types[1184] = (FC_BINARY, timestamptz_recv_float)

                self.py_types[Interval] = (
                    1186, FC_BINARY, interval_send_float)
                self.py_types[Timedelta] = (
                    1186, FC_BINARY, interval_send_float)
                self.pg_types[1186] = (FC_BINARY, interval_recv_float)

        elif key == b"server_version":
            self._server_version = LooseVersion(value.decode('ascii'))
            if self._server_version < LooseVersion('8.2.0'):
                self._commands_with_count = (
                    b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH")
            elif self._server_version < LooseVersion('9.0.0'):
                self._commands_with_count = (
                    b"INSERT", b"DELETE", b"UPDATE", b"MOVE", b"FETCH",
                    b"COPY")

    def array_inspect(self, value):
        # Check if array has any values. If empty, we can just assume it's an
        # array of strings
        first_element = array_find_first_element(value)
        if first_element is None:
            oid = 25
            # Use binary ARRAY format to avoid having to properly
            # escape text in the array literals
            fc = FC_BINARY
            array_oid = pg_array_types[oid]
        else:
            # supported array output
            typ = type(first_element)

            if issubclass(typ, int):
                # special int array support -- send as smallest possible array
                # type
                typ = int
                int2_ok, int4_ok, int8_ok = True, True, True
                for v in array_flatten(value):
                    if v is None:
                        continue
                    if min_int2 < v < max_int2:
                        continue
                    int2_ok = False
                    if min_int4 < v < max_int4:
                        continue
                    int4_ok = False
                    if min_int8 < v < max_int8:
                        continue
                    int8_ok = False
                if int2_ok:
                    array_oid = 1005  # INT2[]
                    oid, fc, send_func = (21, FC_BINARY, h_pack)
                elif int4_ok:
                    array_oid = 1007  # INT4[]
                    oid, fc, send_func = (23, FC_BINARY, i_pack)
                elif int8_ok:
                    array_oid = 1016  # INT8[]
                    oid, fc, send_func = (20, FC_BINARY, q_pack)
                else:
                    raise ArrayContentNotSupportedError(
                        "numeric not supported as array contents")
            else:
                try:
                    oid, fc, send_func = self.make_params((first_element,))[0]

                    # If unknown or string, assume it's a string array
                    if oid in (705, 1043, 25):
                        oid = 25
                        # Use binary ARRAY format to avoid having to properly
                        # escape text in the array literals
                        fc = FC_BINARY
                    array_oid = pg_array_types[oid]
                except KeyError:
                    raise ArrayContentNotSupportedError(
                        "oid " + str(oid) + " not supported as array contents")
                except NotSupportedError:
                    raise ArrayContentNotSupportedError(
                        "type " + str(typ) +
                        " not supported as array contents")
        if fc == FC_BINARY:
            def send_array(arr):
                # check that all array dimensions are consistent
                array_check_dimensions(arr)

                has_null = array_has_null(arr)
                dim_lengths = array_dim_lengths(arr)
                data = bytearray(iii_pack(len(dim_lengths), has_null, oid))
                for i in dim_lengths:
                    data.extend(ii_pack(i, 1))
                for v in array_flatten(arr):
                    if v is None:
                        data += i_pack(-1)
                    elif isinstance(v, typ):
                        inner_data = send_func(v)
                        data += i_pack(len(inner_data))
                        data += inner_data
                    else:
                        raise ArrayContentNotHomogenousError(
                            "not all array elements are of type " + str(typ))
                return data
        else:
            def send_array(arr):
                array_check_dimensions(arr)
                ar = deepcopy(arr)
                for a, i, v in walk_array(ar):
                    if v is None:
                        a[i] = 'NULL'
                    elif isinstance(v, typ):
                        a[i] = send_func(v).decode('ascii')
                    else:
                        raise ArrayContentNotHomogenousError(
                            "not all array elements are of type " + str(typ))
                return str(ar).translate(arr_trans).encode('ascii')

        return (array_oid, fc, send_array)

# pg element oid -> pg array typeoid
pg_array_types = {
    16: 1000,
    25: 1009,    # TEXT[]
    701: 1022,
    1043: 1009,
    1700: 1231,  # NUMERIC[]
}


# PostgreSQL encodings:
#   http://www.postgresql.org/docs/8.3/interactive/multibyte.html
# Python encodings:
#   http://www.python.org/doc/2.4/lib/standard-encodings.html
#
# Commented out encodings don't require a name change between PostgreSQL and
# Python.  If the py side is None, then the encoding isn't supported.
pg_to_py_encodings = {
    # Not supported:
    "mule_internal": None,
    "euc_tw": None,

    # Name fine as-is:
    # "euc_jp",
    # "euc_jis_2004",
    # "euc_kr",
    # "gb18030",
    # "gbk",
    # "johab",
    # "sjis",
    # "shift_jis_2004",
    # "uhc",
    # "utf8",

    # Different name:
    "euc_cn": "gb2312",
    "iso_8859_5": "is8859_5",
    "iso_8859_6": "is8859_6",
    "iso_8859_7": "is8859_7",
    "iso_8859_8": "is8859_8",
    "koi8": "koi8_r",
    "latin1": "iso8859-1",
    "latin2": "iso8859_2",
    "latin3": "iso8859_3",
    "latin4": "iso8859_4",
    "latin5": "iso8859_9",
    "latin6": "iso8859_10",
    "latin7": "iso8859_13",
    "latin8": "iso8859_14",
    "latin9": "iso8859_15",
    "sql_ascii": "ascii",
    "win866": "cp886",
    "win874": "cp874",
    "win1250": "cp1250",
    "win1251": "cp1251",
    "win1252": "cp1252",
    "win1253": "cp1253",
    "win1254": "cp1254",
    "win1255": "cp1255",
    "win1256": "cp1256",
    "win1257": "cp1257",
    "win1258": "cp1258",
    "unicode": "utf-8",  # Needed for Amazon Redshift
}


def walk_array(arr):
    for i, v in enumerate(arr):
        if isinstance(v, list):
            for a, i2, v2 in walk_array(v):
                yield a, i2, v2
        else:
            yield arr, i, v


def array_find_first_element(arr):
    for v in array_flatten(arr):
        if v is not None:
            return v
    return None


def array_flatten(arr):
    for v in arr:
        if isinstance(v, list):
            for v2 in array_flatten(v):
                yield v2
        else:
            yield v


def array_check_dimensions(arr):
    if len(arr) > 0:
        v0 = arr[0]
        if isinstance(v0, list):
            req_len = len(v0)
            req_inner_lengths = array_check_dimensions(v0)
            for v in arr:
                inner_lengths = array_check_dimensions(v)
                if len(v) != req_len or inner_lengths != req_inner_lengths:
                    raise ArrayDimensionsNotConsistentError(
                        "array dimensions not consistent")
            retval = [req_len]
            retval.extend(req_inner_lengths)
            return retval
        else:
            # make sure nothing else at this level is a list
            for v in arr:
                if isinstance(v, list):
                    raise ArrayDimensionsNotConsistentError(
                        "array dimensions not consistent")
    return []


def array_has_null(arr):
    for v in array_flatten(arr):
        if v is None:
            return True
    return False


def array_dim_lengths(arr):
    len_arr = len(arr)
    retval = [len_arr]
    if len_arr > 0:
        v0 = arr[0]
        if isinstance(v0, list):
            retval.extend(array_dim_lengths(v0))
    return retval

def decimalToBinary( dec, bitmaplen):
    """This function converts decimal number
    to binary and prints it"""
    bin = []
    while bitmaplen != 0 :
        remainder = dec % 2
        dec = dec // 2
        bin.append(remainder)
        bitmaplen -= 1
    
    return bin

def date2j(y, m, d):

	m12 = int((m - 14) / 12)
	return ((1461*(y+4800+m12))//4 + (367*(m-2-12*(m12)))//12 - (3*((y+4900+m12)//100))//4 + d - 32075)
 

def j2date(jd):

    date = []
    l = jd + 68569
    n = (4 * l) // 146097
    l -= (146097*n + 3) // 4
    i = (4000 * (l + 1)) // 1461001
    l += 31 - (1461*i)//4
    j = (80 * l) // 2447
    d = l - (2447*j)//80
    l = j // 11
    m = (j + 2) - (12 * l)
    y = 100*(n-49) + i + l
	
    date.append(y)
    date.append(m)
    date.append(d)
	
    return date

def time2struct(time):
    
    time_value = []
    time = int(time / 1000000) # NZ microsecs
    
    hour = int(time / 3600)
    time = time % 3600
    minute = int(time / 60)
    second = int(time % 60)
    
    time_value.append(hour)
    time_value.append(minute)
    time_value.append(second)
    
    return time_value

def IntervalToText(interval_time, interval_month):

    fsec = 0
    
    tm, fsec = interval2tm(interval_time, interval_month, fsec)
    
    fsec = fsec / 1000000
    
    return EncodeTimeSpan(tm, fsec)
    
def interval2tm(interval_time, interval_month, fsec):
    
    tmpVal = 0
    time = []
    
    if interval_month != 0 :
        year = int(interval_month / 12)
        mon = interval_month % 12
    else :
        year = 0
        mon = 0
        
    tmpVal = interval_time // 86400000000
    
    if tmpVal != 0 :
        interval_time -= tmpVal * 86400000000
        mday = tmpVal
        
    tmpVal = interval_time // 3600000000
    
    if tmpVal != 0 :
        interval_time -= tmpVal * 3600000000
        hour = tmpVal
        
    tmpVal = interval_time // 60000000
    
    if tmpVal != 0 :
        interval_time -= tmpVal * 60000000
        min = tmpVal
        
    tmpVal = interval_time // 1000000
    
    if tmpVal != 0 :
        interval_time -= tmpVal * 1000000
        sec = tmpVal
    
    time.append(year)
    time.append(mon)
    time.append(mday)
    time.append(hour)
    time.append(min)
    time.append(sec)
    
    fsec = interval_time
    
    return time, fsec
 
def EncodeTimeSpan(tm, fsec):

	# The sign of year and month are guaranteed to match,
	# since they are stored internally as "month".
	# But we'll need to check for is_before and is_nonzero
	# when determining the signs of hour/minute/seconds fields.
	#
    is_before = minus = False
    
    if tm[0] != 0:
        str = "{} year"
        str = str.format(tm[0])
        if abs(tm[0]) != 1 :
            str = str + "s"
        else:
            str = str + ""
            
        if tm[0] < 0:
            is_before = True
        
        is_nonzero = True
        
    if tm[1] != 0 :
    
        if is_nonzero == True :
            str = str + " "
        else:
            str = str + ""
        
        if is_before == True and tm[1] > 0 :
            str = str + "+"
        else:
            str = str + ""
            
        str_mon = "{} mon"
        str_mon = str_mon.format(tm[1])
        
        if abs(tm[1]) != 1 :
            str_mon = str_mon + "s"
        else :
            str_mon = str_mon + ""
        
        str = str + str_mon
		
        if tm[1] < 0:
            is_before = True
        
        is_nonzero = True
        
    if tm[2] != 0 :
    
        if is_nonzero == True:
            str = str + " "
        else :
            str = str + ""
            
        if is_before == True and tm[2] > 0 :
            str = str + "+"
        else :
            str = str + ""
        
        str_day = "{} day"
        str_day = str_day.format(tm[2])
        
        if abs(tm[2]) != 1 :
            str_day = str_day + "s"
        else :
            str_day = str_day + ""
            
        str = str + str_day
        
        if tm[2] < 0:
            is_before = True
            
        is_nonzero = True
        
    if (is_nonzero == False) or (tm[3] != 0) or (tm[4] != 0) or (tm[5] != 0) or (fsec != 0) :
    
        if ((tm[3] < 0) or (tm[4] < 0) or (tm[5] < 0) or (fsec < 0)):
            minus = True
            
        if is_nonzero == True :
            str = str + " "
        else :
            str = str + ""
            
        if minus == True :
            str = str + "-"
        else :
            if is_before == True :
                str = str + "+"
            else :
                str = str + ""
                
        str_hr_min = "{0:02d}:{1:02d}"
        str = str + str_hr_min.format(abs(tm[3]),abs(tm[4]))
        
        is_nonzero = True

		# fractional seconds?
        
        if fsec != 0 :
            fsec += tm[5]
            str_hr_sec = ":{0:09.6f}"
            str = str + str_hr_sec.format(abs(fsec))
            is_nonzero = True
        # otherwise, integer seconds only? 
        elif tm[5] != 0 :
            str_hr_sec = ":{0:02d}"
            str = str + str_hr_sec.format(abs(tm[5]))
            is_nonzero = True

	# identically zero? then put in a unitless zero... 
    if is_nonzero == False :
        str = str + "0"
        
    return str

def abs(n) :
    if n < 0:
        return -n
    else:
        return n
	 
def timetz_out_timetzadt(timetz_time, timetz_zone):

    tm = []
    
    time = int(timetz_time / 1000000) 
    fusec = timetz_time % 1000000
    
    hour = int(time / 3600)
    time = time % 3600
    min = int(time / 60)
    sec = time % 60
    
    tm.append(hour)
    tm.append(min)
    tm.append(sec)
    
    return EncodeTimeOnly(tm, fusec, timetz_zone)

# EncodeTimeOnly()
# Encode time fields only.
 
def EncodeTimeOnly(tm, fusec, timetz_zone) :

    if (tm[0] < 0) or (tm[0] > 24):
        return ""
    
    if (tm[1] < 0) or (tm[1] > 59):
        return ""
    
    fusec = fusec/1000000
    
    str = "{0:02d}:{1:02d}"
    str = str.format(tm[0], tm[1])
    	
    # fractional seconds? 
    if fusec != 0 :
        fusec += tm[2]
        str_sec = ":{0:09.6f}"
        str = str + str_sec.format(fusec)
    elif tm[2] != 0 :
        str_sec = ":{0:02d}"
        str = str + str_sec.format(tm[2])
        
    if timetz_zone != 0 :
    
        hour = -int(timetz_zone / 3600)
        temp = int(timetz_zone / 60)
        
        if temp < 0 :
            temp = -temp
            
        min = int(temp % 60)
        
        if (hour == 0) and (timetz_zone > 0) :
            str_tz = "-00:{0:02d}"
            str = str + str_tz.format(min)
        else:
            if min != 0 :
                str_tz = "+{0:02d}:{1:02d}"
                str = str + str_tz.format(hour, min)
            else :
                str_tz = "+{0:02d}"
                str = str + str_tz.format(hour)
    
    return str


def timestamp2struct(dt):

    ts = []
    date = int(dt / 86400000000)
    date0 = date2j(2000, 1, 1)
    
    time = dt % 86400000000
    
    if time < 0 :
        time += 86400000000 # NZ - was 86400 w/o exp
        date -= 1	

	# Julian day routine does not work for negative Julian days 
    if date < -date0 :
        return False	

	# add offset to go from J2000 back to standard Julian date
    date += date0
    
    ts = j2date(date)
    
    fraction = (time % 1000000) # NZ microsecs
	
	#Netezza stores the fraction field of TIMESTAMP_STRUCT to
	#microsecond precision. The fraction field of a must be in
	#billionths, per ODBC spec. Therefore, multiply by 1000.
    
    fraction *= 1000
    
    time = int(time/1000000) # NZ microsecs
    
    hour = int(time / 3600)
    time -= (hour * 3600)
    minute = int(time / 60)
    second = time - (minute * 60)

    ts.append(hour)
    ts.append(minute)
    ts.append(second)
    ts.append(fraction)
    
    return ts
