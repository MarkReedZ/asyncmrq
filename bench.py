
import asyncio, time
import asyncmrq, mrpacker

async def run(loop):
  c = asyncmrq.Client()
  #await c.connect(io_loop=loop,servers=[("127.0.0.1",7100), ("127.0.0.1",7002)])
  await c.connect(io_loop=loop,servers=[("127.0.0.1",7100)])

  bstr = mrpacker.pack([1,2,3,4,5,6,7,8,9,10])
  l = len(bstr)

  #await c.bench()

  start = time.time()
  for x in range(1000000):
    #if x % 100000 == 0: print (x)
    #await c.push( 0, 0, bstr, l )
    await c.push_noflush( x, 0, bstr, l )
      #print("push failed")
      #break

  await c.flush()
  end = time.time()
  print(end - start)
  #await c.bench()

  await asyncio.sleep(0.5, loop=loop)
  await c.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
