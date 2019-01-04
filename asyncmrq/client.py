
import asyncio, struct
from .parser import Parser
from .errors import *

DEFAULT_PENDING_SIZE = 1024 * 1024
DEFAULT_BUFFER_SIZE = 128 * 1024
DEFAULT_RECONNECT_TIME_WAIT = 2 # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 10
DEFAULT_PING_INTERVAL = 120  # in seconds
DEFAULT_MAX_OUTSTANDING_PINGS = 2
DEFAULT_MAX_PAYLOAD_SIZE = 1024 * 1024
DEFAULT_MAX_FLUSHER_QUEUE_SIZE = 1024

class Server(object):
    """
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reconnects = 0
        self.last_attempt = None
        self.r = None
        self.w = None
        #self.did_connect = False
        #self.discovered = False

class Client(object):
  """
  Asyncio based client for MrQ
  """

  def __repr__(self):
    return "<nats client v{}>".format(__version__)

  def __init__(self):
    self._loop = None

    self._bare_io_reader = None
    self._io_reader = None
    self._bare_io_writer = None
    self._io_writer = None

    self._error_cb = None
    self._disconnected_cb = None
    self._closed_cb = None
    self._reconnected_cb = None

    self.max_data_size = DEFAULT_MAX_PAYLOAD_SIZE

    self.pending = []
    #self.pending = b''
    self.pending_sz = 0

    self.parser = Parser(self)
    self.read_queues = {}
    self.reads_in_flight = []

    self.options = {}
    self.servers = []

  async def connect(self,
              servers=[("127.0.0.1",7000)],
              ssl=None,
              io_loop=None,
              error_cb=None,
              disconnected_cb=None,
              closed_cb=None,
              reconnected_cb=None,
              ping_interval=DEFAULT_PING_INTERVAL,
              max_outstanding_pings=DEFAULT_MAX_OUTSTANDING_PINGS,
              flusher_queue_size=DEFAULT_MAX_FLUSHER_QUEUE_SIZE
             ):
    #self._setup_server_pool(servers)
    self._loop = io_loop or asyncio.get_event_loop()
    self._error_cb = error_cb
    self._closed_cb = closed_cb
    self._reconnected_cb = reconnected_cb
    self._disconnected_cb = disconnected_cb

    self.options["ping_interval"] = ping_interval
    self.options["max_outstanding_pings"] = max_outstanding_pings

    # Queue used to trigger flushes to the socket
    self.flush_queue = asyncio.Queue( maxsize=flusher_queue_size, loop=self._loop)
    self.flusher_task = self._loop.create_task(self.flusher())

    while True:
      #try:
        for s in servers:
          srv = Server( s[0], s[1] )
          srv.r, srv.w = await asyncio.open_connection( s[0], s[1], loop=self._loop, limit=DEFAULT_BUFFER_SIZE, ssl=ssl)
          self.servers.append(srv)

        self.reading_task = self._loop.create_task(self.read_loop(0))


        #self._current_server.reconnects = 0
        break
    #except ErrNoServers as e:
              #if self.options["max_reconnect_attempts"] < 0:
                  ## Never stop reconnecting
                  #continue
              #self._err = e
              #raise e
          #except (OSError, NatsError) as e:
              #self._err = e
              #if self._error_cb is not None:
                  #yield from self._error_cb(e)
#
              ## Bail on first attempt if reconnecting is disallowed.
              #if not self.options["allow_reconnect"]:
                  #raise e
#
              #yield from self._close(Client.DISCONNECTED, False)
              #self._current_server.last_attempt = time.monotonic()
              #self._current_server.reconnects += 1

  async def close(self):
    #if self.is_closed:
      #self._status = status
      #return
    #self._status = Client.CLOSED

    await self.flush_pending()
    if self.reading_task is not None and not self.reading_task.cancelled():
      self.reading_task.cancel()
    if self.flusher_task is not None and not self.flusher_task.cancelled():
      self.flusher_task.cancel()

    # In case there is any pending data at this point, flush before disconnecting
    #if self._pending_data_size > 0:
      #self._io_writer.writelines(self._pending[:])
      #self._pending = []
      #self._pending_data_size = 0
      #await self._io_writer.drain()
    #if self._io_writer is not None:
      #self._io_writer.close()

        #if do_cbs:
            #if self._disconnected_cb is not None:
                #yield from self._disconnected_cb()
            #if self._closed_cb is not None:
                #yield from self._closed_cb()

  async def flushcmd(self, slot, partition):
    bstr = b'\x00\x0A' + struct.pack("=BB",slot,partition)
    await self._send(bstr, 4)
    if self.flush_queue.empty():
      await self.flush_pending()


  async def push(self, slot, partition, data, data_len):
    """
    Sends a PUSH to the slot / partition specified
    """
    #if self.is_closed: raise ErrConnectionClosed
    if data_len > self.max_data_size:
      raise ErrMaxPayload
    await self._push(slot, partition, data, data_len)


  async def _push(self, slot, partition, data, data_size):
    """
    Sends the push cmd
    """
    #print(b'\x00\x01' + bytes( (slot,partition) ) + struct.pack("I",data_size) + data)
    bstr = b'\x00\x01' + struct.pack("=BBI",slot,partition,data_size) + data
    #print(bstr)
    #print(b'\x00\x01' + struct.pack("BBI",slot,partition,data_size))
    #self.stats['out_msgs'] += 1
    #self.stats['out_bytes'] += data_size
    await self._send(bstr, data_size + 8)
    if self.flush_queue.empty():
      await self.flush_pending()

  async def get(self, b):
    """
    Gets
    """
    bstr = b'\x00\x0B' + struct.pack(">H",len(b)) + b
    k = 0xB
    if not k in self.read_queues.keys():
      self.read_queues[k]  = asyncio.Queue( maxsize=1, loop=self._loop )

    await self._send(bstr, len(b)+4)
    if self.flush_queue.empty():
      await self.flush_pending()

    self.reads_in_flight.append(k)
    return await self.read_queues[k].get()

  async def pull(self, slot, partition ):
    """
    Sends the pull cmd and returns the response data when it arrives
    """
    k = (slot,partition) 
    bstr = b'\x00\x02' + bytes(k)
    #self.stats['out_msgs'] += 1
    #self.stats['out_bytes'] += data_size
    if not k in self.read_queues.keys():
      self.read_queues[k]  = asyncio.Queue( maxsize=1, loop=self._loop )

    await self._send(bstr, 4)
    if self.flush_queue.empty():
      await self.flush_pending()

    self.reads_in_flight.append(k)
    return await self.read_queues[k].get()
#TODO To timeout use wait for item = yield from asyncio.wait_for(queue_obj.get(), 0.05)

  async def bench(self):
    """
    Sends a bench cmd 
    """
    bstr = b'\x00\x09\x00\x00'
    await self._send(bstr, 4)
    if self.flush_queue.empty():
      await self.flush_pending()

  async def _send(self, cmd, sz): #, priority=False):
        #if priority:
            #self._pending.insert(0, cmd)
        #else:
    self.pending.append(cmd)
    #self.pending += cmd
    self.pending_sz += sz
    if self.pending_sz > DEFAULT_PENDING_SIZE:
      #print("flush",self.pending_sz)
      await self.flush_pending()

  async def flush_pending(self):
    try:
      await self.flush_queue.put(None)
      #if not self.is_connected: return
    except asyncio.CancelledError:
      pass

  async def flusher(self):
    """
    Waits on the flush queue and writes pending cmds 
    """
    while True:
      #if not self.is_connected or self.is_connecting:
        #break

      try:
        await self.flush_queue.get()

        if self.pending_sz > 0:
          self.servers[0].w.writelines(self.pending)
          #self.servers[0].w.write(self.pending)
          self.pending = []
          #self.pending = b''
          self.pending_sz = 0
          await self.servers[0].w.drain()
      except OSError as e:
        if self._error_cb is not None:
          await self._error_cb(e)
        #self._process_op_err(e)
        break
      except asyncio.CancelledError:
        break

  def process_read(self, msgs):
    k = self.reads_in_flight.pop(0)
    self.read_queues[k].put_nowait(msgs) 

  async def read_loop(self, s):
    """
    Loops reading from the connection and feeding the parser
    In case of error while reading, it will stop running
    and its task has to be rescheduled.
    """
    while True:
      try:
        #should_bail = self.is_closed or self.is_reconnecting
        #if should_bail or self._io_reader is None: break
        #if self.is_connected and self._io_reader.at_eof():
        if self.servers[s].r.at_eof():  
          #TODO reconnect

          #if self._error_cb is not None:
            #yield from self._error_cb(ErrStaleConnection)
          #self._process_op_err(ErrStaleConnection)
          break

        b = await self.servers[s].r.read(DEFAULT_BUFFER_SIZE)
        self.parser.parse(b)
      except ErrProtocol:
        #self._process_op_err(ErrProtocol)
        break
      except OSError as e:
        #self._process_op_err(e)
        break
      except asyncio.CancelledError:
        break
        # except asyncio.InvalidStateError:
        #     pass

