
import asyncio

STALE_CONNECTION = b"'Stale Connection'"
AUTHORIZATION_VIOLATION = b"'Authorization Violation'"


class MrqError(Exception):
    pass

class ErrProtocol(MrqError):
    def __str__(self):
        return "mrq: Error reading response from server"

class ErrConnectionClosed(MrqError):
    def __str__(self):
        return "mrq: Connection Closed"


class ErrSecureConnRequired(MrqError):
    def __str__(self):
        return "mrq: Secure Connection required"


class ErrSecureConnWanted(MrqError):
    def __str__(self):
        return "mrq: Secure Connection not available"


class ErrSecureConnFailed(MrqError):
    def __str__(self):
        return "mrq: Secure Connection failed"

class ErrSlowConsumer(MrqError):
    def __init__(self, slot=None, partition=None):
        self.slot = slot
        self.partition = partition
    def __str__(self):
        return "mrq: Slow Consumer, messages dropped"

class ErrTimeout(asyncio.TimeoutError):
    def __str__(self):
        return "mrq: Timeout"


class ErrBadTimeout(MrqError):
    def __str__(self):
        return "mrq: Timeout Invalid"


class ErrAuthorization(MrqError):
    def __str__(self):
        return "mrq: Authorization Failed"


class ErrNoServers(MrqError):
    def __str__(self):
        return "mrq: No servers available for connection"


class ErrJsonParse(MrqError):
    def __str__(self):
        return "mrq: Connect message, json parse err"


class ErrStaleConnection(MrqError):
    def __str__(self):
        return "mrq: Stale Connection"


class ErrMaxPayload(MrqError):
    def __str__(self):
        return "mrq: Maximum Payload Exceeded"
