
class Parser(object):

  def __init__(self, c=None):
    self.c = c
    self.buf = b''
    self.bl = 0
    self.msglen = 0

  async def parse(self, s, data=b''):
    self.buf += data

    while 1:
      if len(self.buf) < 5: return
      self.msglen = int.from_bytes(self.buf[1:4], byteorder='little')
      if len(self.buf) < self.msglen+5: return

      if self.buf[0] == 0x1:
        i = 5
        msgs = []
        while (i < self.msglen):
          ml = int.from_bytes(self.buf[i:i+3], byteorder='little')
          i+=4
          msgs.append( self.buf[i:i+ml] )
          i += ml
        await self.c.process_read(s, msgs)
        self.buf = self.buf[i:]
      if self.buf[0] == 0x2:
        await self.c.process_read( s, self.buf[5:5+self.msglen] )
        self.buf = self.buf[5+self.msglen:]
       
      
      

     
