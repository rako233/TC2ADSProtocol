"""Exceptions representing errors in the ADS/AMS protocol or data conversion.

All exceptions raised by the pyads library are sub-classed from the
PyadsException.
"""


class PyadsException(Exception):
    """Base exception class for the pyads library."""
    pass


class AdsException(PyadsException):
    """Represents error codes specified in the ADS protocol as Python exception
    """
    def __init__(self, code):
        self.code = code

    def __str__(self):
        if(self.code in self.AdsCodeNumbers):
            return repr(self.AdsCodeNumbers[self.code])
        else:
            return repr("Error Code #%s" % self.code)

    AdsCodeNumbers = {
        0x1: "Internal error",
        0x2: "No Runtime",
        0x3: "Allocation locked memory error",
        0x4: "Insert mailbox error. Reduce the number of ADS calls",
        0x5: "Wrong receive HMSG",
        0x6: "target port not found",
        0x7: "target machine not found",
        0x8: "Unknown command ID",
        0x9: "Bad task ID",
        0xA: "No IO",
        0xB: "Unknown ADS command",
        0xC: "Win 32 error",
        0xD: "Port not connected",
        0xE: "Invalid ADS length",
        0xF: "Invalid ADS Net ID",
        0x10: "Low Installation level",
        0x11: "No debug available",
        0x12: "Port disabled",
        0x13: "Port already connected",
        0x14: "ADS Sync Win32 error",
        0x15: "ADS Sync Timeout",
        0x16: "ADS Sync AMS error",
        0x17: "ADS Sync no index map",
        0x18: "Invalid ADS port",
        0x19: "No memory",
        0x1A: "TCP send error",
        0x1B: "Host unreachable",
        0x700: "error class <device error>",
        0x701: "Service is not supported by server",
        0x702: "invalid index group",
        0x703: "invalid index offset",
        0x704: "reading/writing not permitted",
        0x705: "parameter size not correct",
        0x706: "invalid parameter value(s)",
        0x707: "device is not in a ready state",
        0x708: "device is busy",
        0x709: "invalid context (must be in Windows)",
        0x70A: "out of memory",
        0x70B: "invalid parameter value(s)",
        0x70C: "not found (files, ...)",
        0x70D: "syntax error in command or file",
        0x70E: "objects do not match",
        0x70F: "object already exists",
        0x710: "symbol not found",
        0x711: "symbol version invalid",
        0x712: "server is in invalid state",
        0x713: "AdsTransMode not supported",
        0x714: "Notification handle is invalid",
        0x715: "Notification client not registered",
        0x716: "no more notification handles",
        0x717: "size for watch too big",
        0x718: "device not initialized",
        0x719: "device has a timeout",
        0x71A: "query interface failed",
        0x71B: "wrong interface required",
        0x71C: "class ID is invalid",
        0x71D: "object ID is invalid",
        0x71E: "request is pending",
        0x71F: "request is aborted",
        0x720: "signal warning",
        0x721: "invalid array index",
        0x722: "symbol not active",
        0x723: "access denied",
        0x724: "missing license",
        0x72c: "exception occured during system start",
        0x740: "Error class <client error>",
        0x741: "invalid parameter at service",
        0x742: "polling list is empty",
        0x743: "var connection already in use",
        0x744: "invoke ID in use",
        0x745: "timeout elapsed",
        0x746: "error in win32 subsystem",
        0x747: "Invalid client timeout value",
        0x748: "ads-port not opened",
        0x750: "internal error in ads sync",
        0x751: "hash table overflow",
        0x752: "key not found in hash",
        0x753: "no more symbols in cache",
        0x754: "invalid response received",
        0x755: "sync port is locked",
    }


class PyadsTypeError(PyadsException, TypeError):
    """Raised when a supplied value cannot be converted to the data type of a
    PLC variable."""
    pass
