import logging
import select
import socket
import struct
import threading
import time

from .constants import PYADS_ENCODING
from .adscommands import DeviceInfoCommand
from .adscommands import ReadCommand
from .adscommands import ReadStateCommand
from .adscommands import ReadWriteCommand
from .adscommands import WriteCommand
from .adscommands import WriteControlCommand
from .adsconstants import ADSIGRP_IOIMAGE_RWIB
from .adsconstants import ADSIGRP_IOIMAGE_RWOB
from .adsconstants import ADSIGRP_SYM_HNDBYNAME
from .adsconstants import ADSIGRP_SYM_INFOBYNAMEEX
from .adsconstants import ADSIGRP_SYM_UPLOAD
from .adsconstants import ADSIGRP_SYM_VALBYHND
from .adsconstants import ADSIGRP_SYM_VALBYNAME
from .adsconstants import ADSIGRP_SYM_SUMREAD
from .adsconstants import ADSIGRP_SYM_SUMWRITE
from .adsconstants import ADSIGRP_SYM_SUMREADWRITE
from .adstypeconvert import AdsTypeConvert
from .adsexception import AdsException
from .adsexception import PyadsException
from .amspacket import AmsPacket
from .adssymbol import AdsTypeInfo, AdsTypeInfoList, AdsSymbolInfo, AdsSymbolInfoList, AdsAlignment
from .adssymbol import AdsSymbol, AdsType
from .adsutils import HexBlock

ADS_CHUNK_SIZE_DEFAULT = 1024
ADS_PORT_DEFAULT = 0xBF02


logger = logging.getLogger(__name__)


class AdsClient(object):
    def __init__(self, ads_connection, debug=False):
        self.ads_connection = ads_connection
        # default values
        self.debug = debug
        self.ads_index_group_in = ADSIGRP_IOIMAGE_RWIB
        self.ads_index_group_out = ADSIGRP_IOIMAGE_RWOB
        self.socket = None
        self._current_invoke_id = 0x8000
        self._current_packet = None
        # event to signal shutdown to async reader thread
        self._stop_reading = threading.Event()

        # lock to ensure only one command is executed
        # (sent to the PLC) at a time:
        self._ads_lock = threading.Lock()

    # BEGIN Connection Management Functions

    @property
    def is_connected(self):
        return self.socket is not None

    def close(self):
        if (self.socket is not None):
            # stop async reading thread
            self._stop_reading.set()
            try:
                self._async_read_thread.join()
            except RuntimeError:
                # ignore Runtime error raised if thread doesn't exist
                pass
            # close socket
            self.socket.close()
            self.socket = None

    def connect(self):
        self.close()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(2)
        try:
            self.socket.connect(
                (self.ads_connection.target_ip, ADS_PORT_DEFAULT))
        except Exception as ex:
            # If an error occurs during connection, close the socket
            # and set it to None so that is_connected() returns False:
            self.socket.close()
            self.socket = None
            raise PyadsException(
                "Could not connect to device: {ex}".format(ex=ex))

        try:
            # start reading thread
            self._stop_reading.clear()
            self._async_read_thread = threading.Thread(
                target=self._async_read_fn)
            self._async_read_thread.daemon = True
            self._async_read_thread.start()
        except Exception as ex:
            raise Exception("Could not start read thread: {ex}".format(ex=ex))

    def _async_read_fn(self):
        while not self._stop_reading.is_set():
            ready = select.select([self.socket], [], [], 0.1)
            if ready[0] and self.is_connected:
                try:
                    newPacket = self.read_ams_packet_from_socket()
                    if (newPacket.invoke_id == self._current_invoke_id):
                        self._current_packet = newPacket
                    else:
                        logger.debug("Packet dropped: %s" % newPacket)
                except socket.error:
                    self.close()
                    break

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, traceback):
        if (ex_type is not None):
            logger.warning(
                f"AdsClient exiting with exception: {ex_type}, {ex_value or ''}. "
                "{traceback or ''}")
        try:
            self.close()
        except:
            pass

    # END Connection Management Methods

    # BEGIN Read/Write Methods

    def execute(self, command):
        with self._ads_lock:
            # create packet
            packet = command.to_ams_packet(self.ads_connection)

            # send to client
            responsePacket = self.send_and_recv(packet)
            # check for error
            if (responsePacket.error_code > 0):
                raise AdsException(responsePacket.error_code)

            # return response object
            result = command.CreateResponse(responsePacket)
            if (result.Error > 0):
                raise AdsException(result.Error)

            return result

    def read_device_info(self):
        cmd = DeviceInfoCommand()
        return self.execute(cmd)

    def read(self, indexGroup, indexOffset, length):
        cmd = ReadCommand(indexGroup, indexOffset, length)
        return self.execute(cmd)

    def write(self, indexGroup, indexOffset, data):
        cmd = WriteCommand(indexGroup, indexOffset, data)
        return self.execute(cmd)

    def read_state(self):
        cmd = ReadStateCommand()
        return self.execute(cmd)

    def write_control(self, adsState, deviceState, data=''):
        cmd = WriteControlCommand(adsState, deviceState, data)
        return self.execute(cmd)

    def read_write(self, indexGroup, indexOffset, readLen, dataToWrite=''):
        cmd = ReadWriteCommand(indexGroup, indexOffset, readLen, dataToWrite)
        return self.execute(cmd)

    # END Read/Write Methods

    # BEGIN variable access methods

    def get_handle_by_name(self, var_name):
        """Retrieves the internal handle of a symbol identified by symbol name.

        var_name: is of type unicode (or str if only ASCII characters are used)
            Both fully qualified PLC symbol names (e.g. including leading "."
            for global variables) or PLC variable names (the name used in the
            PLC program) are accepted. Names are NOT case-sensitive because the
            PLC converts all variables to all-uppercase internally.
        """
        # convert unicode or ascii input to the Windows-1252 encoding used by
        # the plc
        var_name_enc = var_name.encode(PYADS_ENCODING)
        symbol = self.read_write(
            indexGroup=ADSIGRP_SYM_HNDBYNAME,
            indexOffset=0x0000,
            readLen=4,
            dataToWrite=var_name_enc + b'\x00')
        return struct.unpack("I", symbol.data)[0]

    def get_info_by_name(self, var_name):
        """Retrieves extended symbol information including data type and
        comment for a symbol identified by symbol name.

        var_name: is of type unicode (or str if only ASCII characters are used)
            Both fully qualified PLC symbol names (e.g. including leading "."
            for global variables) or PLC variable names (the name used in the
            PLC program) are accepted. Names are NoT case-sensitive because the
            PLC converts all variables to all-uppercase internally.
        """
        var_name_enc = var_name.encode(PYADS_ENCODING)
        # Note: The length of the output varies based on the length of the data
        # type description and length of the comment (which are specified in
        # the first few bytes of the returned byte string. readLen is therefore
        # set to the maximal value that does not result in an error. It's
        # practical meaning seems to be more of a maxReadLen anyway, because
        # the returned string is only as long as necessary to describe the
        # symbol (instead of being zero-padded, for example).
        resp = self.read_write(
            indexGroup=ADSIGRP_SYM_INFOBYNAMEEX,
            indexOffset=0x0000,
            readLen=0xFFFF,
            dataToWrite=var_name_enc + '\x00')

        # First four bytes are the full length of the variable definition,
        # which in Twincat3 includes a non-constant number of bytes of
        # undocumented purpose. Commenting this out because it's not useful
        # when not reading those undocumented bytes, but keeping around as
        # a reminder that this information exists.
        # read_length = struct.unpack("I", resp.data[0:4])[0]
        index_group = struct.unpack("I", resp.data[4:8])[0]
        index_offset = struct.unpack("I", resp.data[8:12])[0]
        name_length = struct.unpack("H", resp.data[24:26])[0]
        type_length = struct.unpack("H", resp.data[26:28])[0]
        comment_length = struct.unpack("H", resp.data[28:30])[0]

        name_start_ptr = 30
        name_end_ptr = name_start_ptr + name_length
        type_start_ptr = name_end_ptr + 1
        type_end_ptr = type_start_ptr + type_length
        comment_start_ptr = type_end_ptr + 1
        comment_end_ptr = comment_start_ptr + comment_length

        name = resp.data[name_start_ptr:name_end_ptr].decode(
            PYADS_ENCODING).strip(' \t\n\r\0')
        symtype = resp.data[type_start_ptr:type_end_ptr]
        comment = resp.data[comment_start_ptr:comment_end_ptr].decode(
            PYADS_ENCODING).strip(' \t\n\r\0')

        return AdsSymbol(
            index_group, index_offset, name, symtype, comment)

    def read_by_handle(self, symbolHandle, ads_data_type):
        """Retrieves the current value of a symbol identified by its handle.

        ads_data_type: The data type of the symbol must be specified as
            AdsDatatype object.
        """
        assert(isinstance(ads_data_type, AdsTypeConvert))
        response = self.read(
            indexGroup=ADSIGRP_SYM_VALBYHND,
            indexOffset=symbolHandle,
            length=ads_data_type.byte_count)
        data = response.data
        return ads_data_type.unpack(data)

    def read_by_name(self, var_name, ads_data_type):
        """Retrieves the current value of a symbol identified by symbol name.

        var_name: is of type unicode (or str if only ASCII characters are used)
            Both fully qualified PLC symbol names (e.g. including leading "."
            for global variables) or PLC variable names (the name used in the
            PLC program) are accepted. Names are NoT case-sensitive because the
            PLC converts all variables to all-uppercase internally.
        ads_data_type: The data type of the symbol must be specified as
            AdsDatatype object.
        """
        assert(isinstance(ads_data_type, AdsTypeConvert))
        var_name_enc = var_name.encode(PYADS_ENCODING)
        response = self.read_write(
            indexGroup=ADSIGRP_SYM_VALBYNAME,
            indexOffset=0x0000,
            readLen=ads_data_type.byte_count,
            dataToWrite=var_name_enc + b'\x00')
        data = response.data
        return ads_data_type.unpack(data)

    def sum_read(self, adsgroupsymbollist):
        buffer = bytearray(b'\0' * 12 * adsgroupsymbollist.size)
        size = 0
        i = 0
        for k,e in adsgroupsymbollist:
            size += AdsType.size(e.type)
            struct.pack_into('<III',buffer,i*12, e.index, e.offset, AdsType.size(e.type))
            i += 1
        response = self.read_write(
            indexGroup=ADSIGRP_SYM_SUMREAD,
            indexOffset=adsgroupsymbollist.size,
            readLen=size+ adsgroupsymbollist.size * 4,
            dataToWrite=buffer)


        pointer = adsgroupsymbollist.size * 4
        for k,e in adsgroupsymbollist:
            adstype = e.type
            ssize = AdsType.size(adstype)
            if ssize>0:
                e.value = AdsType.formatter(adstype).unpack(response.data[pointer:pointer+ssize])
            pointer += ssize

    def block_read(self, adsgroupsymbollist):
        """Read a memory block instead of doing a sum_read(). This method is cheaper on the PLC. The list with
            symbols has to be ordered and (!) be a part of the same index address space
        """
        first = list(adsgroupsymbollist.db.values())[0]
        last = list(adsgroupsymbollist.db.values())[-1]

        size = last.offset - first.offset + AdsType.size(last.type)

        response = self.read(first.index, first.offset, size)

        pointer = 0
        for k, e in adsgroupsymbollist:
            ssize = AdsType.size(e.type)
            pointer = AdsAlignment.calc_alignment(pointer, ssize)
            e.value = AdsType.formatter(e.type).unpack(response.data[pointer:pointer+ssize])
            pointer += ssize

        return size

    def write_by_handle(self, symbolHandle, ads_data_type, value):
        """Retrieves the current value of a symbol identified by its handle.

        ads_data_type: The data type of the symbol must be specified as
            AdsDatatype object.
        value: must meet the requirements of the ads_data_type. For example,
            integer datatypes will require a number to be passed, etc.
        """
        assert(isinstance(ads_data_type, AdsTypeConvert))
        value_raw = ads_data_type.pack(value)
        self.write(
            indexGroup=ADSIGRP_SYM_VALBYHND,
            indexOffset=symbolHandle,
            data=value_raw)

    def write_by_name(self, var_name, ads_data_type, value):
        """Sets the current value of a symbol identified by symbol name.

        This simply calls get_handle_by_name() first and then uses the handle
        to call write_by_handle().

        var_name: must meet the same requirements as in get_handle_by_name,
            i.e. be unicode or an ASCII-only str.
        ads_data_type: must meet the same requirements as in write_by_handle.
        value: must meet the requirements of the ads_data_type. For example,
            integer datatypes will require a number to be passed, etc.
        """
        symbol_handle = self.get_handle_by_name(var_name)
        self.write_by_handle(symbol_handle, ads_data_type, value)

    def get_types(self):
        """get all types by ADS from a Beckhoff PLC

            The type list isn't resolving structures and arrays. A substitution has to be made later

             :return AdsTypeInfoList
        """
        # Figure out the length of the type table first
        adssymlistdata = self.read(
            indexGroup=0xF00F,  # Not a documented constant
            indexOffset=0x0000,
            length=24)
        type_list_length = struct.unpack("I", adssymlistdata.data[12:16])[0]
        type_count = struct.unpack("I", adssymlistdata.data[8:12])[0]
        symbolistdata = self.read(
            indexGroup=0xF00E,  # Not a documented constant
            indexOffset=0x0000,
            length=type_list_length)
        pointer = 0

        typelist = AdsTypeInfoList()

        for i in range(type_count):
            size = struct.unpack("I", symbolistdata.data[pointer+0:pointer+4])[0]
            adstype = AdsTypeInfo.from_data(symbolistdata.data[pointer:pointer+size])
            if adstype:
                typelist.insert(adstype)
            pointer += size
        typelist.sort()

        return typelist

    def get_symbols(self):
        # Figure out the length of the symbol table first
        resp1 = self.read(
            indexGroup=0xF00F,  # Not a documented constant
            indexOffset=0x0000,
            length=24)
        sym_count = struct.unpack("I", resp1.data[0:4])[0]
        sym_data_length = struct.unpack("I", resp1.data[4:8])[0]
        
        # Get the symbol table
        resp2 = self.read(
            indexGroup=ADSIGRP_SYM_UPLOAD,
            indexOffset=0x0000,
            length=sym_data_length)

        symbollist = AdsSymbolInfoList()

        pointer = 0
        for idx in range(sym_count):
            size = struct.unpack("I", resp2.data[pointer:pointer+4])[0]
            symbollist.insert(AdsSymbolInfo.from_data(resp2.data[pointer:pointer+size]))
            pointer += size
        return symbollist

    # END variable access methods

    def read_ams_packet_from_socket(self):
        # read default buffer
        response = self.socket.recv(ADS_CHUNK_SIZE_DEFAULT)
        # ensure correct beckhoff tcp header
        if(len(response) < 6):
            return None
        # first two bits must be 0
        if (response[0:2] != b'\x00\x00'):
            return None
        # read whole data length
        dataLen = struct.unpack('I', response[2:6])[0] + 6
        # read rest of data, if any
        while (len(response) < dataLen):
            nextReadLen = min(ADS_CHUNK_SIZE_DEFAULT, dataLen - len(response))
            response += self.socket.recv(nextReadLen)
        # cut off tcp-header and return response amspacket
        return AmsPacket.from_binary_data(response[6:])

    def get_tcp_header(self, amsData):
        # pack 2 bytes (reserved) and 4 bytes (length)
        # format _must_ be little endian!
        return struct.pack('<HI', 0, len(amsData))

    def get_tcp_packet(self, amspacket):
        # get ams-data and generate tcp-header
        amsData = amspacket.GetBinaryData()
        tcpHeader = self.get_tcp_header(amsData)
        return tcpHeader + amsData

    def send_and_recv(self, amspacket):
        if not self.is_connected:
            self.connect()
        # prepare packet with invoke id
        self.prepare_command_invoke(amspacket)

        try:
            # send tcp-header and ams-data
            self.socket.send(self.get_tcp_packet(amspacket))
        except Exception as ex:
            self.close()
            raise PyadsException(
                "Could not communicate with device: {ex}".format(ex=ex))

        # here's your packet
        return self.await_command_invoke()

    def prepare_command_invoke(self, amspacket):
        if(self._current_invoke_id < 0xFFFF):
            self._current_invoke_id += 1
        else:
            self._current_invoke_id = 0x8000
        self._current_packet = None
        amspacket.invoke_id = self._current_invoke_id
        if self.debug:
            logger.debug(">>> sending ams-packet:")
            logger.debug(amspacket)

    def await_command_invoke(self):
        # unfortunately threading.event is slower than this oldschool poll :-(
        timeout = 0
        while (self._current_packet is None):
            timeout += 0.001
            time.sleep(0.001)
            if (timeout > 10):
                raise AdsException("Timout: Did not receive ADS Answer!")
        if self.debug:
            logger.debug("<<< received ams-packet:")
            logger.debug(self._current_packet)
        return self._current_packet
