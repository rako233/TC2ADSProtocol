from operator import itemgetter

import struct
import re

from .adsutils import HexBlock
from .constants import PYADS_ENCODING
import ads.adstypeconvert as AdsTypeConvert

class AdsTypeMeta(type):
    """
    This class contains the necessary function __getitem__() for AdsType to give access to the class data
    """
    types = {}

    def __getitem__(cls, typeid):
        return cls.types[typeid]

    def type(cls, adstype):
        return cls.types[adstype][0]

    def size(cls, adstype):
        return cls.types[adstype][1]

    def signed(cls, adstype):
        return cls.types[adstype][2]

    def struct(cls, adstype):
        return cls.types[adstype][3]

    def formatter(cls, adstype):
        return cls.types[adstype][4]

class AdsType(object, metaclass=AdsTypeMeta):
    """ This class is a systematic access to ADS type data. The
    Data is access by the functions defined in the meta class.
    The type TIME is arriving as 0x41 (Type BIG) but it is a unsigned 4 byte value
    based on miliseconds
    """
    types = {
        0x21 : ('BOOL', 1, False, False, AdsTypeConvert.BOOL),
        0x41 : ('STRUCT', 0, False, True, AdsTypeConvert.BYTE),
        0x13 : ('UDINT', 4, False, False, AdsTypeConvert.UDINT),
        0x12 : ('UINT', 2, False, False, AdsTypeConvert.UINT),
        0x11 : ('USINT', 1, False, False, AdsTypeConvert.USINT),
        0x10 : ('SINT', 1, True, False, AdsTypeConvert.SINT),
        0x02 : ('INT', 2, True, False, AdsTypeConvert.INT),
        0x03 : ('DINT', 4, True, False, AdsTypeConvert.DINT),
        0x04 : ('REAL', 4, True, False, AdsTypeConvert.REAL),
        0x05 : ('LREAL', 8, True, False, AdsTypeConvert.LREAL),
        0x1E : ('STRING', 0, False, False, AdsTypeConvert.STRING)
    }

class AdsTypeInfoList(object):
    """
    This class manages all AdsTypeInfo objects from the incomming ADS data block.
    """
    def __init__(self):
        self.db = dict()
        self.sorted = False

    def insert(self, symboltype):
        self.db[symboltype.path] = symboltype
        self.sorted = False;

    def __getitem__(self, item):
        return self.db[item]

    def __iter__(self):
        return self.db.items().__iter__()

    def __contains__(self, key):
        return key in self.db

    def sort(self):
        db = dict()
        for ele in sorted(self.db.values(), key=itemgetter('path')):
            db[ele.path] = ele
        self.db = db

    def repr_tree(self, ntab=0):
        res = ''
        for e in self.db.values():
            res += '\t'*ntab + str(e) + '\n'
            if e.isstruct:
                res += e.child.repr_tree(ntab=ntab+1)
        return res

    def __repr__(self):
        res = f'<AdsTypeInfoList Size: {len(self.db)}>\n'
        for k,e in self.db.items():
            res += '\t'+ str(e)+'\n'
        return res

class AdsTypeInfo(object):
    """
    AdsTypeInfo parses data of an ADS type from the ADS data

    type: int : The numerical ads type
    symboltype:  str : The type as a string
    isstruct : bool : set when the type is a struct
    isarray : bool : set when the type is an array
    struct_is_child : bool  : Since TC2 is expanding types in structs this is the solution for the mess
    datasize : int : totalsize of this type
    noelements : int : number of elements in an array
    child : AdsTypeInfoList : A struct can have a list with members
    path : str : the complete type definition
    type : str : the base type of a definition
    comment : str :
    strtype : str : the extracted basetype from path
    """

    _reg_idx = re.compile('ARRAY\s+\[([0-9]+).+([0-9]+)\].+OF\s(\w+)', re.IGNORECASE)

    def __init__(self):
        self.child = AdsTypeInfoList()

    @classmethod
    def from_arg(cls, type, datasize, noelements, path, comment, isstruct=False, isarray=False, ischild=False):
        obj = cls()
        obj.type = type
        obj.symboltype = AdsType.type(type)
        obj.typesize = AdsType.size(type)
        obj.datasize = datasize
        obj.noelements = noelements
        obj.path= path
        obj.comment = comment
        obj.strtype = obj.symboltype
        obj.isstruct = isstruct
        obj.isarray = isarray
        obj.ischild = ischild
        obj.signed = AdsType.signed(type)

        return obj

    @classmethod
    def from_data(cls, adsdata):
        obj = cls()
        adstype = adsdata[0x18]
        obj.type = adstype
        if not (adstype in AdsType.types):
            raise TypeError(f'The ads type 0x{adstype:20x} is not implemented!')
        else:
            obj.symboltype = AdsType.type(adstype)

        obj.isstruct = True if adsdata[0x18] == 0x41 else False
        obj.datasize = struct.unpack("I", adsdata[0x10:0x14])[0] # Total size
        obj.typesize = AdsType.size(adstype) # The size of one element

        path_length = struct.unpack("H", adsdata[0x20:0x22])[0]
        type_length = struct.unpack("H", adsdata[0x22:0x24])[0]
        comment_length = struct.unpack("H", adsdata[0x24:0x26])[0]

        pointer = 0x2a
        obj.path = adsdata[pointer:pointer + path_length].decode(PYADS_ENCODING, errors='ignore').strip(' \t\n\r\0')
        pointer += path_length + 1
        obj.strtype = adsdata[pointer:pointer + type_length].decode(PYADS_ENCODING, errors='ignore').strip(' \t\n\r\0')
        pointer += type_length + 1

        obj.comment = adsdata[pointer + comment_length + 1 : pointer + comment_length + 1 + comment_length].decode(
            PYADS_ENCODING, errors='ignore').strip(' \t\n\r\0')
        pointer += comment_length + 1

        # This block parses the array information. Because types are different treated in structures
        # the test on array is different inside structures. The type string in strtype is replaced by the base type of
        # an array
        obj.struct_is_child = False

        arraydata  = cls._reg_idx.findall(obj.path)
        if arraydata:
            ll, ul, arraytype = arraydata[0]
            obj.strtype = arraytype
            obj.noelements = int(ul) - int(ll) + 1
            obj.isarray = True
        else:
            arraydata = cls._reg_idx.findall(obj.strtype)
            if arraydata:
                ll, ul, arraytype = arraydata[0]
                obj.struct_is_child = True
                obj.isarray = True
                obj.strtype = arraytype
                obj.noelements = int(ul) - int(ll) + 1
            else:
                obj.isarray = False
                obj.noelements = 1

        obj.signed = AdsType.signed(adstype)

        if obj.isstruct:
            if not obj.struct_is_child:
                obj.nomember = struct.unpack("H", adsdata[0x28:0x2A])[0]
                for i in range(obj.nomember):
                    msize = struct.unpack("I", adsdata[pointer + 0x00:pointer + 0x04])[0]
                    obj.child.insert(AdsTypeInfo.from_data(adsdata[pointer:-1]))
                    pointer += msize
        else:
            obj.nomember = 0

        return obj

    def __getitem__(self, item):
        return self.__dict__[item]

    def __repr__(self):
        return  f'<{self.path}'\
                f' T {self.type}'\
                f' TSz {self.datasize}'\
                f' ISz {self.typesize}'\
                f' #{self.noelements}'\
                f'{" ST" if self.isstruct else ""}'\
                f' {"S" if self.signed else ""}>'

class AdsSymbolInfoList(object):
    """
    This class is a container for AdsSymbolInfo
    """
    def __init__(self):
        self.db = dict()
        self.sorted = False

    def insert(self, symbol):
        self.db[symbol.path] = symbol
        self.sorted = False;

    def __getitem__(self, item):
        return self.db[item]

    def __iter__(self):
        return self.db.items().__iter__()

    def __contains__(self, key):
        return key in self.db

    def __repr__(self):
        res = f'<AdsSymbolInfoList Size: {len(self.db)}>\n'
        for k,e in self.db.items():
            res += '\t'+ str(e)+'\n'
        return res

class AdsSymbolInfo(object):
    """AdsSymbolInfo collects all data of an ADS type"""

    @classmethod
    def from_data(cls, adsdata):
        obj = cls()
        path_length = struct.unpack("H", adsdata[0x18: 0x1A])[0]
        type_length = struct.unpack("H", adsdata[0x1A:0x1C])[0]
        comment_length = struct.unpack("H", adsdata[0x1C:0x1E])[0]

        obj.index = struct.unpack("I", adsdata[0x04:0x08])[0]
        obj.offset = struct.unpack("I", adsdata[0x08:0x0C])[0]
        obj.datasize = struct.unpack("I", adsdata[0x0C:0x10])[0]
        obj.type = struct.unpack("H", adsdata[0x10:0x12])[0]

        pointer = 0x1E
        obj.path = adsdata[pointer:pointer + path_length].decode(PYADS_ENCODING, errors='ignore').strip(' \t\n\r\0')
        pointer += path_length + 1
        obj.typesymbol = adsdata[pointer:pointer+type_length].decode(PYADS_ENCODING, errors='ignore').strip(' \t\n\r\0')
        pointer += type_length + 1
        obj.comment = adsdata[pointer:pointer+comment_length].decode(PYADS_ENCODING, errors='ignore')

        obj.isstruct = False
        obj.isarray = False

        return obj

    def __getitem__(self, item):
        return self.__dict__[item]

    def __repr__(self):
        return f'<{self.path}> idx {self.index:04x}:{self.offset:04x} Sz {self.datasize} T {self.typesymbol} T#{self.type:02x} "{self.comment}"'

class AdsSymbol(object):
    """This class represents a simple ADS symbol.Arrays and structs are already substituted"""

    def __init__(self, path, index, offset, adstype):
        self.path = path
        self.index = index
        self.offset = offset
        self.type = adstype
        self.value = None

    def __getitem__(self, item):
        return self.__dict__[item]

    def __repr__(self):
        return f'<[{self.index:04x}:{self.offset:04x}] {self.path} {self.value if not (self.value is None) else "[NoValue]"} >'

class AdsGroupSymbolList(object):
    pass

class AdsAlignment(object):
    """Helper class to calculate alignments"""

    #These are constants for calculation of alignments for TC2 on Arm
    _omask = {  0:{'mask':0b11, 'off':4},
                1:{'mask':0b00, 'off':1},
                2:{'mask':0b01, 'off':2},
                4:{'mask':0b11, 'off':4},
                8:{'mask':0b11, 'off':4}}

    @classmethod
    def calc_alignment(cls, offset, size):
        """Get the next aligned offset

        """

        if ((offset & cls._omask[size]['mask']) > 0):
            offset = (offset & (~cls._omask[size]['mask'])) + cls._omask[size]['off']
        return offset

class AdsSymbolList(object):
    """
    This is a container for AdsSymbol and contains all functionality to get list with AdsSymbols
    """
    #These are constants for calculation of alignments for TC2 on Arm
    _omask = {  0:{'mask':0b11, 'off':4},
                1:{'mask':0b00, 'off':1},
                2:{'mask':0b01, 'off':2},
                4:{'mask':0b11, 'off':4},
                8:{'mask':0b11, 'off':4}}

    def __init__(self, typeinfolist, symbolinfolist, alignment=True):

        self.db = dict()
        self.tinfolist = typeinfolist
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x13, AdsType.size(0x13), 1, 'UDINT', 'UDINT', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x13, AdsType.size(0x13), 1, 'TIME', 'TIME', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x02, AdsType.size(0x13), 1, 'INT', 'INT', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x21, AdsType.size(0x21), 1, 'BOOL', 'BOOL', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x12, AdsType.size(0x12), 1, 'UINT', 'UINT', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x11, AdsType.size(0x11), 1, 'USINT', 'USINT', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x10, AdsType.size(0x10), 1, 'SINT', 'SINT', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x02, AdsType.size(0x02), 1, 'INT', 'INT', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x03, AdsType.size(0x03), 1, 'DINT', 'DINT', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x04, AdsType.size(0x04), 1, 'REAL', 'REAL', ''))
        self.tinfolist.insert(AdsTypeInfo.from_arg(0x05, AdsType.size(0x05), 1, 'LREAL', 'LREAL', ''))
        #self.tinfolist.insert(AdsTypeInfo.from_arg(0x1E, AdsType.size(0x1E), 1, 'STRING', 'STRING', ''))

        self.sinfolist = symbolinfolist
        self.alignment = alignment

        self.build_symbol_list()

    def __getitem__(self, item):
        return self.db[item]

    def __iter__(self):
        return self.db.items().__iter__()

    def __contains__(self, key):
        return key in self.db

    def calc_alignment(self, offset, size):
        """Get the next aligned offset

        """
        if self.alignment:
            offset = AdsAlignment.calc_alignment(offset, size)
        return offset

    def insert(self, symbol : AdsSymbol):
        """Insert an AdsSymbol"""
        self.db[symbol.path] = symbol

    def _symbol_simple_by_sinfo(self, sinfopath : str, master : str = ''):
        """Make a simple symbol from ADS symbol data"""
        sinfo = self.sinfolist[sinfopath]
        path = f'{master}{"." if master else ""}{sinfopath}'
        offset = sinfo.offset
        self.insert(AdsSymbol(path, sinfo.index, offset, sinfo.type))

    def _symbol_simple_by_tinfo(self, tinfo : AdsTypeInfo, master : str, index : int, offset : int):
        """Make a simple symbol from Ads type data"""
        path = f'{master}.{tinfo.path}'
        offset = self.calc_alignment(offset, AdsType.size(tinfo.type))
        self.insert(AdsSymbol(path, index.idx, offset, tinfo.type))
        offset += AdsType(tinfo.type)

    def _symbol_struct_by_tinfo(self, tinfo: AdsTypeInfo, master : str, index : int, offset : int):
        """Produce for each member a symbol. When it's a structure go
         recursive into the structure

         Because the alignment of the structure depends on the first element the offset has to set by

                self._symbol_struct_by_tinfo()

        """
        for k, e in tinfo.child:
            path = f'{master}.{e.path}'
            if not (e.isarray or e.isstruct):
                offset = self.calc_alignment(offset, AdsType.size(e.type))
                self.insert(AdsSymbol(path, index, offset, e.type))
                offset += AdsType.size(e.type)
            else:
                if e.isarray:
                    offset = self.calc_alignment(offset, AdsType.size(e.type))
                    self._symbol_array_by_tinfo(e, path, index, offset)
                    offset += e.datasize
                elif e.isstruct:
                    #This is tricky, because the alignment of a structure in TC2 depends on the first member.
                    try:
                        tinfostruct = self.tinfolist[e.strtype]
                        offset = self._symbol_struct_by_tinfo(tinfostruct, path, index, offset)
                    except:
                        # Unknown data type.
                        offset = self.calc_alignment(offset, AdsType.size(e.type))
                        offset += e.datasize
        # Return the offset for the next symbol
        return offset


    def _symbol_array_by_tinfo(self, tinfo: AdsTypeInfo, master : str, index : int, offset : int):
        """Construct members of an array"""
        for i in range(tinfo.noelements):
            path = f'{master}[{i}]'
            if not tinfo.isstruct:
                # do alignment if necessary
                offset = self.calc_alignment(offset, AdsType.size(tinfo.type))
                self.insert(AdsSymbol(path, index, offset, tinfo.type))
                offset += AdsType.size(tinfo.type)
            else: # Dive recursive into a structure. Don't insert the struct itself.

                # Because the alignment of the structure depends on the first element
                # the offset has to set by self._symbol_struct_by_tinfo()
                tinfostruct = self.tinfolist[tinfo.strtype]
                offset = self._symbol_struct_by_tinfo(tinfostruct, path, index, offset)

    def build_symbol_list(self):
        """Build the complete symbol list. Arrays and structures are dissolved"""
        for k,e in self.sinfolist:
            if e.typesymbol in self.tinfolist:
                tinfo = self.tinfolist[e.typesymbol]
                if tinfo.isarray:
                    self._symbol_array_by_tinfo(tinfo, e.path, e.index, e.offset)
                elif tinfo.isstruct:
                    self._symbol_struct_by_tinfo(tinfo, e.path, e.index, e.offset)
                else: # case of a sub type of integer
                    self._symbol_simple_by_sinfo(k)
            else:
                self._symbol_simple_by_sinfo(k)

    def filter(self, keylist : list) -> AdsGroupSymbolList:
        """Seach for symbols with a list of regular expressions. The search is not case sensitive! Each member of
        the list is equivalent to a level of the hierachy. The resultr is a list with can be extended to avoid
        too complex regular expressions. Example:

            filter = ['ssensor','ss_pump\[0\]']
            my_symbol_list = symbollist.filter(filter)

            This gives all sensor values of ss_pump[0]. The resulting access path is

            .SSENSOR.SS_PUMP[0].<member of struct>
        """
        path = ''
        result = AdsGroupSymbolList()

        for s in keylist:
            path += '.'+ s
        filter = re.compile(path, re.IGNORECASE)
        for k, e in self.db.items():
            if filter.match(e.path):
                result.insert(e)
        return result


    def __repr__(self):
        res = ''
        for k,e in self.db.items():
            res += str(e)+'\n'
        return res

class AdsGroupSymbolList(object):
    """
    This is a container for AdsSymbols
    """

    def __init__(self):
        self.db = dict()
        self.size = 0

    def __getitem__(self, item):
        return self.db[item]

    def __iter__(self):
        return self.db.items().__iter__()

    def __contains__(self, key):
        return key in self.db

    def insert(self, symbol):
        self.db[symbol.path] = symbol
        self.size += 1

    def extend_list(self, grouplist):
        for k, e in grouplist.db.items():
            self.size += 1
            self.db[k] = e

    def __repr__(self):
        res = f'{self.__class__}\n'
        for k,e in self.db.items():
            res += '\t'+str(e)+'\n'
        return res