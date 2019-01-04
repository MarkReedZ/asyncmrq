
class Parser(object):

  def __init__(self, c=None):
    self.c = c
    self.buf = b''
    self.bl = 0
    self.msglen = 0

  def parse(self, data=b''):
    """
    Parses the response from the MrQ server
    """
    self.buf += data
    if self.bl == 0:
      self.msglen = int.from_bytes(data[1:4], byteorder='little')
    self.bl += len(data)

    if self.bl < self.msglen+5: return

    #b'\x01\x08\x00\x00\x00\x04\x00\x00\x00test'
    #print( data[0] )
    #print( int.from_bytes(data[1:4], byteorder='little') )
    #print( int.from_bytes(data[5:8], byteorder='little') )

    if self.buf[0] == 0x1:
      i = 5
      msgs = []
      while (i < self.msglen):
        ml = int.from_bytes(self.buf[i:i+3], byteorder='little')
        i+=4
        msgs.append( self.buf[i:i+ml] )
        i += ml
      self.c.process_read(msgs)
    if self.buf[0] == 0x2:
      self.c.process_read( self.buf[5:5+self.msglen] )
       
      
      

     
