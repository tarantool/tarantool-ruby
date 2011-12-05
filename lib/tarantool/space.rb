module Tarantool
  class Space
    attr_accessor :space_no
    attr_reader :connection
    def initialize(connection, space_no = nil)
      @connection = connection
      @space_no = space_no
    end

    def select(*args)
      request Requests::Select, args
    end

    def call(*args)
      request Requests::Call, args
    end

    def insert(*args)
      request Requests::Insert, args
    end


    def delete(*args)
      request Requests::Delete, args
    end

    def update(*args)
      request Requests::Update, args
    end

    def ping(*args)
      request Requests::Ping, args
    end

    def request(cls, args)      
      cls.new(self, *args).perform
    end
  end
end