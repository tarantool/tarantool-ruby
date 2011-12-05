module EventMachine
  module Protocols
    module FixedHeaderAndBody

      def self.included(base)
        base.extend ClassMethods
      end

      module ClassMethods
        def header_size(size = nil)
          if size
            @_header_size = size
          else
            @_header_size
          end
        end
      end

      attr_accessor :header, :body

      def receive_data(data)
        @buffer ||= ''
        offset = 0
        while (chunk = data[offset, _needed_size - @buffer.size]).size > 0 || _needed_size == 0
          @buffer += chunk
          offset += chunk.size
          if @buffer.size == _needed_size
            case _state
            when :receive_header
              @_state = :receive_body
              receive_header @buffer
            when :receive_body
              @_state = :receive_header
              receive_body @buffer
            end
            @buffer = ''
          end
        end 
      end

      def receive_header(header)
        # for override
      end

      def body_size
        # for override
      end

      def receive_body(body)
        # for override
      end

      def _needed_size
        case _state
        when :receive_header
          self.class.header_size
        when :receive_body
          body_size
        end
      end

      def _state
        @_state ||= :receive_header
      end
    end
  end
end