"""Collection of utility functions for data types available in Twincat.

A documentation of Twincat data types is available at
http://infosys.beckhoff.com/content/1033/tcplccontrol/html/tcplcctrl_plc_data_types_overview.htm?id=20295  # nopep8
"""
from collections import OrderedDict
from collections import Sequence
from copy import copy
import datetime
from functools import reduce
import struct

from .constants import PYADS_ENCODING
from .adsexception import PyadsTypeError


class AdsTypeConvert(object):
    """Represents a simple data type with a fixed byte count."""
    def __init__(self, byte_count, pack_format):
        self.byte_count = int(byte_count)
        self.pack_format = str(pack_format)

    def pack(self, values_list):
        """Pack a value using Python's struct.pack()"""
        assert(self.pack_format is not None)
        return struct.pack(self.pack_format, *values_list)

    def pack_into_buffer(self, byte_buffer, offset, values_list):
        assert(self.pack_format is not None)
        struct.pack_into(self.pack_format, byte_buffer, offset, *values_list)

    def unpack(self, value):
        """Unpack a value using Python's struct.unpack()"""
        assert(self.pack_format is not None)
        # Note: "The result is a tuple even if it contains exactly one item."
        # (https://docs.python.org/2/library/struct.html#struct.unpack)
        # For single-valued data types, use AdsSingleValuedDatatype to get the
        # first (and only) entry of the tuple after unpacking.
        return struct.unpack(self.pack_format, value)

    def unpack_from_buffer(self, byte_buffer, offset):
        assert(self.pack_format is not None)
        return struct.unpack_from(self.pack_format, byte_buffer, offset)


class AdsSingleValuedTypeConvert(AdsTypeConvert):
    """Represents Twincat's variable types that are NOT arrays."""
    def pack(self, value):
        return super(AdsSingleValuedTypeConvert, self).pack([value])

    def pack_into_buffer(self, byte_buffer, offset, value):
        return super(AdsSingleValuedTypeConvert, self).pack(
            byte_buffer, offset, [value])

    def unpack(self, value):
        unpacked_tuple = super(AdsSingleValuedTypeConvert, self).unpack(value)
        return unpacked_tuple[0]

    def unpack_from_buffer(self, byte_buffer, offset):
        unpacked_tuple = super(
            AdsSingleValuedTypeConvert, self).unpack(byte_buffer, offset)
        return unpacked_tuple[0]


class AdsStringDatatype(AdsSingleValuedTypeConvert):
    """Represents Twincat's variable length STRING data type."""
    def __init__(self, str_length=80):
        super(AdsStringDatatype, self).__init__(
            byte_count=str_length, pack_format='%ss' % str_length)

    def byte_str_to_decoded_str(self, byte_str):
        return byte_str.split('\x00', 1)[0].decode(PYADS_ENCODING)

    def pack(self, value):
        # encode in Windows-1252 encoding
        value = value.encode(PYADS_ENCODING)
        return super(AdsStringDatatype, self).pack(value)

    def pack_into_buffer(self, byte_buffer, offset, value):
        # encode in Windows-1252 encoding
        value = value.encode(PYADS_ENCODING)
        super(AdsStringDatatype, self).pack_into_buffer(
            byte_buffer, offset, value)

    def unpack(self, value):
        """Unpacks the value into a byte string of str_length, then
        drops all bytes after and including the first NULL character.
        """
        value = super(AdsStringDatatype, self).unpack(value)
        return self.byte_str_to_decoded_str(value)

    def unpack_from_buffer(self, byte_buffer, offset):
        """c.f. unpack()"""
        value = super(AdsStringDatatype, self).unpack_from_buffer(
            byte_buffer, offset)
        return self.byte_str_to_decoded_str(value)


class AdsTimeDatatype(AdsSingleValuedTypeConvert):
    """Represents Twincat's TIME data type."""
    def __init__(self):
        # DATE, TIME, and DATE_AND_TIME are all handled as WORD by Twincat
        super(AdsTimeDatatype, self).__init__(byte_count=4, pack_format='I')

    def time_to_milliseconds_integer(self, value):
        """Converts a Python datetime.time object to an integer.

        The output represents the number of milliseconds since
        datetime.time(0). Any time zone information is ignored.
        """
        assert(isinstance(value, datetime.time))
        return (
            ((value.hours * 60 + value.minutes) * 60 + value.seconds) * 1000 +
            int(value.microseconds / 1000))

    def milliseconds_integer_to_time(self, value):
        """Converts an integer into a Python datetime.time object.

        The input is assumed to represent the number of milliseconds since
        datetime.time(0). Any time zone information is ignored.
        """
        assert(isinstance(value, int))
        # pretend this is a timestamp in millisecond resolution and get the
        # datetime ignoring timezones, then discard the date component
        dt = datetime.datetime.utcfromtimestamp(value / 1000.0)
        return dt.time()

    def pack(self, value):
        value = self.time_to_milliseconds_integer(value)
        return super(AdsTimeDatatype, self).pack(value)

    def pack_into_buffer(self, byte_buffer, offset, value):
        value = self.time_to_milliseconds_integer(value)
        super(AdsTimeDatatype, self).pack_into_buffer(
            byte_buffer, offset, value)

    def unpack(self, value):
        value = super(AdsTimeDatatype, self).unpack(value)
        return self.milliseconds_integer_to_time(value)

    def unpack_from_buffer(self, byte_buffer, offset):
        value = super(AdsTimeDatatype, self).unpack_from_buffer(
            byte_buffer, offset)
        return self.milliseconds_integer_to_time(value)


class AdsDateDatatype(AdsSingleValuedTypeConvert):
    def __init__(self):
        # DATE, TIME, and DATE_AND_TIME are all handled as WORD by Twincat
        super(AdsDateDatatype, self).__init__(byte_count=4, pack_format='I')

    # contrary to what the docs say the resolution of the DATE datatype is
    # one day, not one second
    def time_to_days_integer(self, value):
        assert(isinstance(value, datetime.date))
        dt1970 = datetime.date(1970, 1, 1)
        tdelta = value - dt1970
        return tdelta.days

    def days_integer_to_time(self, value):
        assert(isinstance(value, int))
        dt1970 = datetime.date(1970, 1, 1)
        return dt1970 + datetime.date(days=value)

    def pack(self, value):
        value = self.time_to_days_integer(value)
        return super(AdsTimeDatatype, self).pack(value)

    def pack_into_buffer(self, byte_buffer, offset, value):
        value = self.time_to_days_integer(value)
        super(AdsTimeDatatype, self).pack_into_buffer(
            byte_buffer, offset, value)

    def unpack(self, value):
        value = super(AdsTimeDatatype, self).unpack(value)
        return self.days_integer_to_time(value)

    def unpack_from_buffer(self, byte_buffer, offset):
        value = super(AdsTimeDatatype, self).unpack_from_buffer(
            byte_buffer, offset)
        return self.days_integer_to_time(value)


# TODO
class AdsDateAndTimeDatatype(AdsSingleValuedTypeConvert):
    def __init__(self):
        # DATE, TIME, and DATE_AND_TIME are all handled as WORD by Twincat
        super(AdsDateAndTimeDatatype, self).__init__(
            byte_count=4, pack_format='I')

    def pack(self, value):
        pass

    def pack_into_buffer(self, byte_buffer, offset, value):
        pass

    def unpack(self, value):
        pass

    def unpack_from_buffer(self, byte_buffer, offset):
        pass


class AdsArrayTypeConvert(AdsTypeConvert):
    """Factory for data types represented as arrays in PLC code:
    'ARRAY [0..3,1..4] OF UINT'.
    """
    def __init__(self, data_type, dimensions=None):
        """Creates data type capable of packing and unpacking an array of
        elements of a single-valued data type. The Python representation of
        the array is as a dict because PLC arrays are arbitrarily indexed.
        Multidimensional PLC arrays are represented as nested dicts.

        data_type must be of type AdsSingleValuedDatatype
        dimensions is either the total number of elements in the array as
            integer or a list of tuple of (inclusive) start and end indices in
            the same order as they appear in the array definition in PLC code
        """
        assert(isinstance(data_type, AdsSingleValuedTypeConvert))

        # if the array is 1-dimensional and zero-indexed the dimensions
        # argument could be an integer
        if isinstance(dimensions, int):
            self.dimensions = [(0, dimensions - 1)]  # 0..n => n+1 elements!
        elif isinstance(dimensions, list):
            self.dimensions = dimensions
        else:
            raise TypeError(
                "The dimensions parameter must be either int or a list of "
                "tuples. %s was given." % type(dimensions))

        # calculate the total number of elements in the array, keeping in mind
        # that it could be multidimensional
        self.total_element_count = reduce(
            lambda x, y: x * (y[1] - y[0] + 1),  # 1..4 => 4 elements!
            dimensions, 1)

        total_byte_count = self.total_element_count * data_type.byte_count
        super(AdsArrayTypeConvert, self).__init__(
            byte_count=total_byte_count,
            pack_format='{cnt}{fmt}'.format(
                cnt=self.total_element_count,
                fmt=data_type.pack_format,
            ))

    def _dict_to_flat_list(self, dict_, dims=None):
        """Recursively builds a flat list from a dict while checking if the
        dict's keys match the array specification. The returned data type is
        tuple.

        For example, an integer array specified as [(0, 2), (7,9)] is correctly
        represented by a dict of this structure:
        {0: {7: a, 8: b, 9: c}, 1: {7: d, 8: e, 9: f}, 2: {7: g, 8: h, 9: i}}
        or this list/tuple: [a, b, c, d, e, f, g, h, i]

        If dims is not provided, self.dimensions is used. When the function
        calls itself, it passes a truncated copy its own version of dims.
        """
        # initialize the flattened list as empty
        flat = []
        # operate on a local copy of dims list to not modify the version
        # used by the calling function (which in many cases will be another
        # branch of the recursive tree)
        dims = copy(dims or self.dimensions)
        # pop from the left to get the index bounds of the dimension of the
        # array we are currently validating, while shortening dims for
        # validation of the next dimension
        try:
            cur_dims = dims.pop(0)
        except (AttributeError, KeyError):
            raise PyadsTypeError(
                "Failed to pop the first entry off the list of array "
                "dimensions.")
        try:
            indices = sorted(dict_.keys())
        except AttributeError:
            raise PyadsTypeError(
                "Failed to find array keys from dict representation.")
        # perform validation for current dimension
        if min(indices) != cur_dims[0]:
            raise PyadsTypeError(
                "Expected lowest index %d but found %d." %
                (cur_dims[0], min(indices)))
        if max(indices) != cur_dims[1]:
            raise PyadsTypeError(
                "Expected highgest index %d but found %d." %
                (cur_dims[1], max(indices)))
        if len(indices) != max(indices) - min(indices) + 1:
            raise PyadsTypeError(
                "All indices between and including {mn} and {mx} must be "
                "present but only {lst} are.".format(
                    mn=min(indices),
                    mx=max(indices),
                    lst=','.join(map(str, indices))))
        # can't iterate over dict_.values(), they might not be in order,
        # iterate over sorted indices instead
        for idx in indices:
            if len(dims) > 0:
                flat += self._dict_to_flat_list(dict_[idx], dims)
            else:
                flat.append(dict_[idx])
        return tuple(flat)

    def _flat_list_to_dict(self, flat, dims=None):
        """Inverse of _dict_to_flat_list: Recursively builds a dict from a flat
        list using the array spec.

        If dims is not provided, self.dimensions is used. When the function
        calls itself, it passes a truncated copy its own version of dims.
        """
        if not isinstance(flat, Sequence):
            raise PyadsTypeError(
                "Array data must be a sequence (list, tuple, string), but %s "
                "was given." % type(flat))
        # For recursion to work on multi-dimensional arrays, all nodes of the
        # recursion tree must work on the same mutable sequence (list). For
        # that to work, convert to list iff flat is not a list. Always calling
        # list() would result in each call to this function operating on a
        # different copy of the input sequence. This conversion is generally
        # useful because struct.unpack(), which is where the input to this
        # function usually originates, returns a tuple.
        if not isinstance(flat, list):
            flat = list(flat)
        # operate on a local copy of dims list to not modify the version
        # used by the calling function (which in many cases will be another
        # branch of the recursive tree)
        dims = copy(dims or self.dimensions)
        # pop from the left to get the index bounds of the array dimension we
        # are currently building, while shortening dims for the next dimension
        cur_dims = dims.pop(0)
        # Recursively step through the array specification (in dims) and pop
        # elements from the flat list into the dict.
        assert(cur_dims[0] <= cur_dims[1])
        dict_ = OrderedDict()
        for idx in range(cur_dims[0], cur_dims[1] + 1):
            if len(dims) > 0:
                dict_[idx] = self._flat_list_to_dict(flat, dims)
            else:
                # As opposed to the dims array, here we actually want to modify
                # the list globally across all branches of the recursion.
                try:
                    dict_[idx] = flat.pop(0)
                except IndexError:
                    raise PyadsTypeError(
                        'The array data from the PLC has fewer elements than '
                        'required by the array specification.')
        return dict_

    def pack(self, value):
        """Packs the Python representation of the array into a binary string.

        As a convenience, both the dict representation returned by unpack() and
        a flattened list are accepted as inputs.
        """
        # The exception message for incorrect arguments can get complex here,
        # pre-assemble a base message first, then modify it for each specific
        # exception.
        dims = len(self.dimensions)
        exception_str = """The Python representation of this PLC array variable
        must either be a sequence (tuple, list, string) of length {list_len} or
        a {nested} dict with keys {dict_keys}. %s""".format(
            list_len=self.total_element_count,
            nested="%d-fold nested " % dims if dims > 1 else "",
            dict_keys=','.join(["%d..%d" % bnds for bnds in self.dimensions]))

        # Check the value argument for correct type. If it's a dict, check the
        # dict keys against the array dimensions. After all checks, convert the
        # input value into a flattened array.
        if isinstance(value, Sequence):
            # Check for correct list length
            if len(value) != self.total_element_count:
                raise PyadsTypeError(
                    exception_str %
                    "The supplied list has %d elements." %
                    len(value))
            # Nothing else to do in this branch, the array is already a
            # flattened list.
            flat = value
        elif isinstance(value, dict):
            # Recursively flatten the dict into a list
            try:
                flat = self._dict_to_flat_list(value)
            except PyadsTypeError as ex:
                raise PyadsTypeError(exception_str % ex.message)
        else:
            raise PyadsTypeError(
                exception_str % "The value must be a list or a dict.")

        return super(AdsArrayTypeConvert, self).pack(flat)

    def unpack(self, value):
        flat = super(AdsArrayTypeConvert, self).unpack(value)
        return self._flat_list_to_dict(flat)


BOOL = AdsSingleValuedTypeConvert(byte_count=1, pack_format='?')  # Bool
BYTE = AdsSingleValuedTypeConvert(byte_count=1, pack_format='b')  # Int8
WORD = AdsSingleValuedTypeConvert(byte_count=2, pack_format='H')  # UInt16
DWORD = AdsSingleValuedTypeConvert(byte_count=4, pack_format='I')  # UInt32
SINT = AdsSingleValuedTypeConvert(byte_count=1, pack_format='b')  # Int8 (Char)
USINT = AdsSingleValuedTypeConvert(byte_count=1, pack_format='B')  # UInt8
INT = AdsSingleValuedTypeConvert(byte_count=2, pack_format='h')  # Int16
INT16 = INT  # Int16
UINT = AdsSingleValuedTypeConvert(byte_count=2, pack_format='H')  # UInt16
UINT16 = UINT  # UInt16
DINT = AdsSingleValuedTypeConvert(byte_count=4, pack_format='i')  # Int32
UDINT = AdsSingleValuedTypeConvert(byte_count=4, pack_format='I')  # UInt32
# LINT (64 Bit Integer, not supported by TwinCAT)
# ULINT (Unsigned 64 Bit Integer, not supported by TwinCAT)
REAL = AdsSingleValuedTypeConvert(byte_count=4, pack_format='f')  # float
LREAL = AdsSingleValuedTypeConvert(byte_count=8, pack_format='d')  # double
STRING = AdsStringDatatype
# Duration time. The most siginificant digit is one millisecond. The data type
# is handled internally like DWORD.
TIME = AdsTimeDatatype()
TIME_OF_DAY = TIME  # only semantically different from TIME
TOD = TIME_OF_DAY  # alias
DATE = AdsDateDatatype()
DATE_AND_TIME = AdsDateAndTimeDatatype()
DT = DATE_AND_TIME  # alias
