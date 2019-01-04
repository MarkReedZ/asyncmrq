
import asyncio, time, ssl
import asyncmrq

async def run(loop):
  c = asyncmrq.Client()
  await c.connect(io_loop=loop,servers=[("127.0.0.1",7100)])

  #sc = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
  #sc.load_verify_locations('cert/server.crt')
  #await c.connect(io_loop=loop,servers=[("127.0.0.1",7100)],ssl=sc)

  await c.push( 0, 0, b'[1,2]', 5 )

  await asyncio.sleep(0.5, loop=loop)

  #msgs = await c.pull( 0, 0 )
  #print(msgs)

  await c.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()

