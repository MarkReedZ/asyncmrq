
import asyncio, struct
from .parser import Parser
from .errors import *

DEFAULT_PENDING_SIZE = 1024 * 1024
DEFAULT_BUFFER_SIZE = 128 * 1024
DEFAULT_MAX_FLUSHER_QUEUE_SIZE = 1024

class Server(object):
    """
    """

    def __init__(self, host, port, loop):
        self.host = host
        self.port = port
        self.reconnects = 0
        self.last_attempt = None
        self.r = None
        self.w = None
        self.flush_queue = asyncio.Queue( maxsize=1, loop=loop )
        self.read_queue  = asyncio.Queue( maxsize=2, loop=loop )
        self.pending = []
        self.pending_sz = 0
        self.connected = False
        self.reconnect_task = None
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

    self._error_cb = None
    self._disconnected_cb = None
    self._closed_cb = None
    self._reconnected_cb = None

    self.parser = Parser(self)

    self.servers = []

  async def connect(self,
              servers=[("127.0.0.1",7000)],
              ssl=None,
              io_loop=None,
              error_cb=None,
              disconnected_cb=None,
              closed_cb=None,
              reconnected_cb=None,
              flusher_queue_size=DEFAULT_MAX_FLUSHER_QUEUE_SIZE
             ):
    #self._setup_server_pool(servers)
    self._loop = io_loop or asyncio.get_event_loop()
    self._error_cb = error_cb
    self._closed_cb = closed_cb
    self._reconnected_cb = reconnected_cb
    self._disconnected_cb = disconnected_cb
    self.reconnect_task = None
    self.ssl = ssl

    self.read_tasks  = []
    self.flush_tasks = []

    self.max_pending = 1024 * 1024 * 10

    while True:
      #try:
        for s in servers:
          srv = Server( s[0], s[1], self._loop )
          srv.r, srv.w = await asyncio.open_connection( s[0], s[1], loop=self._loop, limit=DEFAULT_BUFFER_SIZE, ssl=ssl)
          srv.connected = True
          self.servers.append(srv)

        self.num_servers = len(self.servers)

        for n in range(len(servers)):
          self.read_tasks.append( self._loop.create_task(self.read_loop(n)) )
          self.flush_tasks.append( self._loop.create_task(self.flusher(n)) )


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

    for s in range(len(self.servers)):
      await self.flush_pending(s)
    for t in self.read_tasks:
      t.cancel()
    for t in self.flush_tasks:
      t.cancel()
    #if self.reading_task is not None and not self.reading_task.cancelled():
      #self.reading_task.cancel()
    #if self.flusher_task is not None and not self.flusher_task.cancelled():
      #self.flusher_task.cancel()

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

  async def send_flush(self, slot, partition):
    bstr = b'\x00\x0A' + struct.pack("=BB",slot,partition)
    s = slot % len(self.servers)
    self.servers[s].pending.append(bstr)
    self.servers[s].pending_sz += 4
    if self.servers[s].flush_queue.empty():
      await self.flush_pending(s)


  async def push(self, slot, partition, data, data_len):
    """
    Sends a PUSH to the slot / partition specified
    """
    s = slot % self.num_servers
    if not self.servers[s].connected:
      if self.servers[s].pending_sz > self.max_pending:
        return -1
    #if self.is_closed: raise ErrConnectionClosed
    #if data_len > self.max_data_size: raise ErrMaxPayload
    #await self._push(slot, partition, data, data_len)
    bstr = b'\x00\x01' + struct.pack("=BBI",slot,partition,data_len) + data
    self.servers[s].pending.append(bstr)
    self.servers[s].pending_sz += data_len + 8
    if self.servers[s].flush_queue.empty():
      await self.flush_pending(s)

  async def push_noflush(self, slot, partition, data, data_len):
    """
    Sends a PUSH to the slot / partition specified
    """
    s = slot % self.num_servers
    if not self.servers[s].connected:
      if self.servers[s].pending_sz > self.max_pending:
        return -1
    #if self.is_closed: raise ErrConnectionClosed
    #if data_len > self.max_data_size: raise ErrMaxPayload
    bstr = b'\x00\x01' + struct.pack("=BBI",s,partition,data_len) + data
    self.servers[s].pending.append(bstr)
    self.servers[s].pending_sz += data_len + 8
    if self.servers[s].pending_sz > DEFAULT_PENDING_SIZE:
      await self.flush_pending(s)
    return 0


  async def get(self, s, b):
    s = s % self.num_servers
    bstr = b'\x00\x0B' + struct.pack(">H",len(b)) + b
    k = 0xB

    self.servers[s].pending.append(bstr)
    self.servers[s].pending_sz += len(b)+4
    if self.servers[s].flush_queue.empty():
      await self.flush_pending(s)

    return await self.servers[s].read_queue.get()

  async def pull(self, slot, partition ):
    s = slot % self.num_servers
    k = (slot,partition) 
    bstr = b'\x00\x02' + bytes(k)

    self.servers[s].pending.append(bstr)
    self.servers[s].pending_sz += 4
    if self.servers[s].flush_queue.empty():
      await self.flush_pending(s)

    return await self.servers[s].read_queue.get()

  async def bench(self):
    """
    Sends a bench cmd 
    """
    s = 0 #TODO
    bstr = b'\x00\x09\x00\x00'
    self.servers[s].pending.append(bstr)
    self.servers[s].pending_sz += 4
    if self.servers[s].flush_queue.empty():
      await self.flush_pending(s)

  async def flush(self):
    for s in range(len(self.servers)):
      await self.flush_pending(s)

  async def flush_pending(self, s):
    try:
      if not self.servers[s].connected: return
      await self.servers[s].flush_queue.put(None)
    except asyncio.CancelledError:
      pass

  async def flusher(self, s):
    while True:
      #if not self.is_connected or self.is_connecting:
        #break

      try:
        await self.servers[s].flush_queue.get()

        if self.servers[s].pending_sz > 0:
          self.servers[s].w.writelines(self.servers[s].pending)
          self.servers[s].pending = []
          self.servers[s].pending_sz = 0
          await self.servers[s].w.drain()
      except OSError as e:
        #if self._error_cb is not None:
          #await self._error_cb(e)
        #self._process_op_err(e)
        print(e)
        if self.reconnect_task == None:
          self.reconnect_task = self.reconnect(s)
        await self.reconnect_task
      except asyncio.CancelledError:
        break

  async def reconnect(self, s):
    srv = self.servers[s]
    srv.connected = False
    while True:
      try:
        print("Attempting to reconnect to server",s)
        srv.r, srv.w = await asyncio.open_connection( srv.host, srv.port, loop=self._loop, limit=DEFAULT_BUFFER_SIZE, ssl=self.ssl)
        print("Reconnected")
        srv.connected = True
        self.reconnect_task = None
        break
      except:
        await asyncio.sleep(5)

  async def process_read(self, s, msgs):
    await self.servers[s].read_queue.put(msgs) 

  async def read_loop(self, s):
    while True:
      try:
        #should_bail = self.is_closed or self.is_reconnecting
        #if should_bail or self._io_reader is None: break
        #if self.is_connected and self._io_reader.at_eof():
        if self.servers[s].r.at_eof():  
          self.servers[s].connected = False
          if self.reconnect_task == None:
            self.reconnect_task = self.reconnect(s)
          await self.reconnect_task

          #if self._error_cb is not None:
            #yield from self._error_cb(ErrStaleConnection)
          #self._process_op_err(ErrStaleConnection)
          #break

        b = await self.servers[s].r.read(DEFAULT_BUFFER_SIZE) 
        await self.parser.parse(s, b)
      except ErrProtocol:
        #self._process_op_err(ErrProtocol)
        break
      except OSError as e:
        #self._process_op_err(e)
        print("read",e)
        if self.reconnect_task == None:
          self.reconnect_task = self.reconnect(s)
        await self.reconnect_task
      except asyncio.CancelledError:
        break
        # except asyncio.InvalidStateError:
        #     pass

