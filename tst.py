
import asyncio, time, ssl
import asyncmrq
import mrpacker

async def run(loop):
  c = asyncmrq.Client()
  #await c.connect(io_loop=loop,servers=[("127.0.0.1",7100),("127.0.0.1",7002)])
  await c.connect(io_loop=loop,servers=[("127.0.0.1",7100)])

  #sc = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
  #sc.load_verify_locations('cert/server.crt')
  #await c.connect(io_loop=loop,servers=[("127.0.0.1",7100)],ssl=sc)


  #await c.push( 0, 0, m, l )
  #await asyncio.sleep(0.5, loop=loop)
  #await c.close()
  #return
  if 0:
    print( mrpacker.unpack(await c.get( 0, mrpacker.pack([1,2,3])) ))

  if 0:
    l = []
    l.append( c.get( 0, mrpacker.pack([1,2,3])) )
    l.append( c.get( 0, mrpacker.pack([1,2,3,4])) )
    l.append( c.get( 0, mrpacker.pack([5])) )
    print( await asyncio.gather( *l ) )
  
  for x in range(100):
    m = mrpacker.pack(x)
    l = len(m)
    if await c.push( x, 0, m, l ):
      print(x,"droppped")
    else:
      print(x)
    await asyncio.sleep(0.5, loop=loop)

  await asyncio.sleep(0.5, loop=loop)

  #msgs = await c.pull( 0, 0 )
  #print(msgs)

  await c.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()

