
import asyncio, time
import asyncmrq

async def run(loop):
  c = asyncmrq.Client()
  await c.connect(io_loop=loop,servers=[("127.0.0.1",7100)])

  bstr = b"[1,2,3,4,5,6,7,8,9,10]"#.encode("utf-8")
  l = len(bstr)

  await c.bench()

  start = time.time()
  for x in range(1000000):
    #if x % 100000 == 0: print (x)
    await c.push( 0, 0, bstr, l )
  end = time.time()
  print(end - start)

  await c.bench()

  await asyncio.sleep(0.5, loop=loop)
  await c.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
