
class Parser(object):

  def __init__(self, c=None):
    self.c = c
    self.buf = b''
    self.bl = 0

  async def parse(self, data=b''):
    """
    Parses the response from the MrQ server
    """
    self.buf += data
    self.bl += len(data)
    #b'\x01\x08\x00\x00\x00\x04\x00\x00\x00test'
    print( data[0] )
    print( int.from_bytes(data[1:4], byteorder='little') )
    print( int.from_bytes(data[5:8], byteorder='little') )
    totlen = int.from_bytes(data[1:4], byteorder='little')
    if self.bl < totlen+5:
      return

    i = 5
    msgs = []
    while (i < totlen):
      ml = int.from_bytes(self.buf[i:i+3], byteorder='little')
      i+=4
      msgs.append( self.buf[i:i+ml] )
      i += ml

    self.c.process_read(msgs)
      
      

     
