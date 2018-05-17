
import asyncio
from mrq.client import Client 
#from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run(loop):
  c = Client()
  await c.connect(io_loop=loop)
  await c.push( 0, 0, b'test', 4 )
  await c.push( 0, 0, b'bart', 4 )
  await c.push( 0, 0, b'tart', 4 )
  await c.push( 0, 0, b'cart', 4 )
  await asyncio.sleep(0.5, loop=loop)
  msgs = await c.pull( 0, 0 )
  print(msgs)
  await c.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
