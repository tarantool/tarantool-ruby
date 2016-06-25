module Tarantool16
  IPROTO_CODE = 0x00
  IPROTO_SYNC = 0x01
  IPROTO_SPACE_ID = 0x10
  IPROTO_INDEX_ID = 0x11
  IPROTO_LIMIT = 0x12
  IPROTO_OFFSET = 0x13
  IPROTO_ITERATOR = 0x14
  IPROTO_KEY = 0x20
  IPROTO_TUPLE = 0x21
  IPROTO_FUNCTION_NAME = 0x22
  IPROTO_USER_NAME = 0x23
  IPROTO_EXPR = 0x27
  IPROTO_DEF_DUPLE = 0x28
  IPROTO_DATA = 0x30
  IPROTO_ERROR = 0x31

  IPROTO_GREETING_SIZE = 128

  REQUEST_TYPE_OK = 0
  REQUEST_TYPE_PING = 64
  REQUEST_TYPE_SELECT = 1
  REQUEST_TYPE_INSERT = 2
  REQUEST_TYPE_REPLACE = 3
  REQUEST_TYPE_UPDATE = 4
  REQUEST_TYPE_DELETE = 5
  REQUEST_TYPE_CALL = 6
  REQUEST_TYPE_AUTHENTICATE = 7
  REQUEST_TYPE_EVAL = 8
  REQUEST_TYPE_UPSERT = 9
  REQUEST_TYPE_ERROR = 1 << 15


  SPACE_SCHEMA  = 272
  SPACE_SPACE   = 280
  SPACE_VSPACE  = 281
  SPACE_INDEX   = 288
  SPACE_VINDEX  = 289
  SPACE_FUNC    = 296
  SPACE_USER    = 304
  SPACE_PRIV    = 312
  SPACE_CLUSTER = 320

  INDEX_SPACE_PRIMARY = 0
  INDEX_SPACE_NAME    = 2
  INDEX_INDEX_PRIMARY = 0
  INDEX_INDEX_NAME    = 2

  ITERATOR_EQ  = 0
  ITERATOR_REQ = 1
  ITERATOR_ALL = 2
  ITERATOR_LT  = 3
  ITERATOR_LE  = 4
  ITERATOR_GE  = 5
  ITERATOR_GT  = 6
  ITERATOR_BITS_ALL_SET     = 7
  ITERATOR_BITS_ANY_SET     = 8
  ITERATOR_BITS_ALL_NOT_SET = 9
  ITERATOR_RTREE_OVERLAPS = 10
  ITERATOR_RTREE_NEIGHBOR = 11

  Iterators = {}
  [
    [ITERATOR_EQ, %w[eq ==]],
    [ITERATOR_REQ, %w[req rev ==<]],
    [ITERATOR_ALL, %w[all *]],
    [ITERATOR_LT, %w[<  lt]],
    [ITERATOR_LE, %w[<= le]],
    [ITERATOR_GE, %w[>= ge]],
    [ITERATOR_GT, %w[>  gt]],
    [ITERATOR_BITS_ALL_SET, %w[ball &=]],
    [ITERATOR_BITS_ANY_SET, %w[bany &]],
    [ITERATOR_BITS_ALL_NOT_SET, %w[bnotany !&]],
    [ITERATOR_RTREE_OVERLAPS, %w[roverlaps here &&]],
    [ITERATOR_RTREE_NEIGHBOR, %w[rneighbor near <->]],
  ].each do |it, names|
    names.each do |name|
      Iterators[it] = it
      Iterators[name] = it
      Iterators[name.to_sym] = it
    end
  end
  Iterators[nil] = ITERATOR_EQ
  def self.iter(iter)
    unless it = Iterators[iter]
      raise "Unknown iterator #{iter.inspect}"
    end
    it
  end

  # Default value for socket timeout (seconds)
  SOCKET_TIMEOUT = nil
  # Default maximum number of attempts to reconnect
  RECONNECT_MAX_ATTEMPTS = 10
  # Default delay between attempts to reconnect (seconds)
  RECONNECT_DELAY = 0.1
  # Number of reattempts in case of server
  # return completion_status == 1 (try again)
  RETRY_MAX_ATTEMPTS = 10
end
