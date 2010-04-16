#!/usr/bin/env ruby
#
# This is a simplified form of thrift, useful for clients only, and not
# making any attempt to have good performance. It's intended to be used by
# small command-line tools that don't want to install a dozen ruby files.
#

require 'socket'
require 'getoptlong'

module SimpleThrift
  VERSION_1 = 0x8001

  # message types
  CALL, REPLY, EXCEPTION = (1..3).to_a

  # value types
  STOP, VOID, BOOL, BYTE, DOUBLE, _, I16, _, I32, _, I64, STRING, STRUCT, MAP, SET, LIST = (0..15).to_a

  FORMATS = {
    BYTE => "c",
    DOUBLE => "G",
    I16 => "n",
    I32 => "N",
  }

  SIZES = {
    BYTE => 1,
    DOUBLE => 8,
    I16 => 2,
    I32 => 4,
  }

  module ComplexType
    module Extends
      def type_id=(n)
        @type_id = n
      end

      def type_id
        @type_id
      end
    end

    module Includes
      def to_i
        self.class.type_id
      end

      def to_s
        args = self.values.map { |v| self.class.type_id == STRUCT ? v.name : v.to_s }.join(", ")
        "#{self.class.name}.new(#{args})"
      end
    end
  end

  def self.make_type(type_id, name, *args)
    klass = Struct.new("STT_#{name}", *args)
    klass.send(:extend, ComplexType::Extends)
    klass.send(:include, ComplexType::Includes)
    klass.type_id = type_id
    klass
  end

  ListType = make_type(LIST, "ListType", :element_type)
  MapType = make_type(MAP, "MapType", :key_type, :value_type)
  StructType = make_type(STRUCT, "StructType", :struct_class)

  class << self
    def pack_value(type, value)
      case type
      when BOOL
        [ value ? 1 : 0 ].pack("c")
      when STRING
        [ value.size, value ].pack("Na*")
      when I64
        [ value >> 32, value & 0xffffffff ].pack("NN")
      when ListType
        [ type.element_type.to_i, value.size ].pack("cN") + value.map { |item| pack_value(type.element_type, item) }.join("")
      when MapType
        [ type.key_type.to_i, type.value_type.to_i, map.size ].pack("ccN") + map.map { |k, v| pack_value(type.key_type, k) + pack_value(type.value_type, v) }.join("")
      when StructType
        value._pack
      else
        [ value ].pack(FORMATS[type])
      end
    end

    def read_value(s, type)
      case type
      when BOOL
        s.read(1).unpack("c").first != 0
      when STRING
        len = s.read(4).unpack("N").first
        s.read(len)
      when I64
        hi, lo = s.read(8).unpack("NN")
        (hi << 32) | lo
      when LIST
        read_list(s)
      when MAP
        read_map(s)
      when STRUCT
        read_struct(s)
      when ListType
        read_list(s, type.element_type)
      when MapType
        read_map(s, type.key_type, type.value_type)
      when StructType
        read_struct(s, type.struct_class)
      else
        s.read(SIZES[type]).unpack(FORMATS[type]).first
      end
    end

    def read_list(s, element_type=nil)
      etype, len = s.read(5).unpack("cN")
      expected_type = (element_type and element_type.to_i == etype.to_i) ? element_type : etype
      rv = []
      len.times do
        rv << read_value(s, expected_type)
      end
      rv
    end

    def read_map(s, key_type=nil, value_type=nil)
      ktype, vtype, len = s.read(6).unpack("ccN")
      rv = {}
      expected_key_type, expected_value_type = if key_type and value_type and key_type.to_i == ktype and value_type.to_i = vtype
        [ key_type, value_type ]
      else
        [ ktype, vtype ]
      end
      len.times do
        key = read_value(s, expected_key_type)
        value = read_value(s, expected_value_type)
        rv[key] = value
      end
      rv
    end

    def read_struct(s, struct_class=nil)
      rv = struct_class ? struct_class.new() : nil
      while true
        type = s.read(1).unpack("c").first
        return rv if type == STOP
        fid = s.read(2).unpack("n").first
        field = struct_class ? struct_class._fields.find { |f| (f.fid == fid) and (f.type.to_i == type) } : nil
        value = read_value(s, field ? field.type : type)
        rv[field.name] = value if field
      end
    end

    def read_response(s, rv_class)
      version, message_type, method_name_len = s.read(8).unpack("nnN")
      method_name = s.read(method_name_len)
      seq_id = s.read(4).unpack("N").first
      [ method_name, seq_id, read_struct(s, rv_class).rv ]
    end
  end


  ## ----------------------------------------

  class Field
    attr_accessor :name, :type, :fid

    def initialize(name, type, fid)
      @name = name
      @type = type
      @fid = fid
    end

    def pack(value)
      value.nil? ? "" : [ type.to_i, fid, SimpleThrift.pack_value(type, value) ].pack("cna*")
    end
  end


  class ThriftException < RuntimeError
    def initialize(reason)
      @reason = reason
    end

    def to_s
      "ThriftException(#{@reason.inspect})"
    end
  end


  module ThriftStruct
    module Include
      def _pack
        self.class._fields.map { |f| f.pack(self[f.name]) }.join + [ STOP ].pack("c")
      end
    end

    module Extend
      def _fields
        @fields
      end

      def _fields=(f)
        @fields = f
      end
    end
  end


  def self.make_struct(name, *fields)
    names = fields.map { |f| f.name.to_sym }
    klass = Struct.new("ST_#{name}", *names)
    klass.send(:include, ThriftStruct::Include)
    klass.send(:extend, ThriftStruct::Extend)
    klass._fields = fields
    klass
  end


  class ThriftService
    def initialize(sock)
      @sock = sock
    end

    def self._arg_structs
      @_arg_structs = {} if @_arg_structs.nil?
      @_arg_structs
    end

    def self.thrift_method(name, rtype, *args)
      arg_struct = SimpleThrift.make_struct("Args__#{self.name}__#{name}", *args)
      rv_struct = SimpleThrift.make_struct("Retval__#{self.name}__#{name}", SimpleThrift::Field.new(:rv, rtype, 0))
      _arg_structs[name.to_sym] = [ arg_struct, rv_struct ]

      arg_names = args.map { |a| a.name.to_s }.join(", ")
      class_eval "def #{name}(#{arg_names}); _proxy(:#{name}#{args.size > 0 ? ', ' : ''}#{arg_names}); end"
    end

    def _proxy(method_name, *args)
      cls = self.class.ancestors.find { |cls| cls.respond_to?(:_arg_structs) and cls._arg_structs[method_name.to_sym] }
      arg_class, rv_class = cls._arg_structs[method_name.to_sym]
      arg_struct = arg_class.new(*args)
      data = [ VERSION_1, CALL, method_name.to_s.size, method_name.to_s, 0, arg_struct._pack ].pack("nnNa*Na*")
      @sock.write(data)
      rv = SimpleThrift.read_response(@sock, rv_class)
      rv[2]
    end

    # convenience. robey is lazy.
    [[ :field, "Field.new" ], [ :struct, "StructType.new" ],
     [ :list, "ListType.new" ], [ :map, "MapType.new" ]].each do |new_name, old_name|
      class_eval "def self.#{new_name}(*args); SimpleThrift::#{old_name}(*args); end"
    end

    [ :void, :bool, :byte, :double, :i16, :i32, :i64, :string ].each { |sym| class_eval "def self.#{sym}; SimpleThrift::#{sym.to_s.upcase}; end" }
  end
end
