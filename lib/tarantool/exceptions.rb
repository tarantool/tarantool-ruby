module Tarantool
  class ConnectionError < ::IProto::Disconnected; end
  class NoMasterError < ConnectionError; end

  class ArgumentError < ::ArgumentError; end
  class StringTooLong < ArgumentError; end
  class IntegerFieldOverflow < ArgumentError; end

  class TarantoolError < StandardError; end
  class ValueError < TarantoolError; end
  class StatusCode < TarantoolError
    attr_reader :code
    def initialize(code, msg)
      super(msg)
      @code = code
    end
    def to_s
      "#{super} [#{code}]"
    end
  end
  # try again return codes
  class TryAgain < StatusCode; end
  class TupleReadOnly < TryAgain; end
  class TupleIsLocked < TryAgain; end
  class MemoryIssue   < TryAgain; end
  # general error return codes
  class BadReturnCode < StatusCode; end
  class NonMaster     < BadReturnCode; end
  class IsSecondaryPort < NonMaster; end
  class IllegalParams < BadReturnCode; end
  class BadIntegrity  < BadReturnCode; end
  class UnsupportedCommand < BadReturnCode; end
  class WrongField    < BadReturnCode; end
  class WrongNumber   < BadReturnCode; end
  class Duplicate     < BadReturnCode; end # it is rather useful
  class WrongVersion  < BadReturnCode; end
  class WalIO         < BadReturnCode; end
  class LuaError      < BadReturnCode; end
  class StoredProcedureNotDefined < BadReturnCode; end
  class TupleExists   < BadReturnCode; end
  class TupleDoesntExists < BadReturnCode; end
  class DuplicateKey  < BadReturnCode; end
  CODE_TO_EXCEPTION = {
    0x0401 => TupleReadOnly,
    0x0601 => TupleIsLocked,
    0x0701 => MemoryIssue,
    0x0102 => NonMaster,
    0x0202 => IllegalParams,
    0x0302 => IsSecondaryPort,
    0x0802 => BadIntegrity,
    0x0a02 => UnsupportedCommand,
    0x1e02 => WrongField,
    0x1f02 => WrongNumber,
    0x2002 => Duplicate,
    0x2602 => WrongVersion,
    0x2702 => WalIO,
    0x3102 => TupleDoesntExists,
    0x3202 => StoredProcedureNotDefined,
    0x3302 => LuaError,
    0x3702 => TupleExists,
    0x3802 => DuplicateKey,
  }
  CODE_TO_EXCEPTION.default = BadReturnCode
end
