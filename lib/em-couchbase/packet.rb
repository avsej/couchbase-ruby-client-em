# Author:: Couchbase <info@couchbase.com>
# Copyright:: 2012 Couchbase, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module EventMachine
  module Protocols

    module Couchbase

      module Packet

        #   uint8_t   magic
        #   uint8_t   opcode
        #   uint16_t  keylen
        #   uint8_t   extlen
        #   uint8_t   datatype
        #   uint16_t  vbucket
        #   uint32_t  bodylen
        #   uint32_t  opaque
        #   uint64_t  cas
        REQUEST_HEADER_FMT = "CCnCCnNNQ"
        REQUEST_HEADER_SIZE = 24  # bytes

        # Sum of lengths of fields below
        #   uint8_t   magic
        #   uint8_t   opcode
        #   uint16_t  keylen
        #   uint8_t   extlen
        #   uint8_t   datatype
        #   uint16_t  status
        #   uint32_t  bodylen
        #   uint32_t  opaque
        #   uint64_t  cas
        RESPONSE_HEADER_FMT = "CCnCCnNNQ"
        RESPONSE_HEADER_SIZE = 24 # bytes

        CMD_GET = 0x00
        CMD_SET = 0x01
        CMD_ADD = 0x02
        CMD_REPLACE = 0x03
        CMD_DELETE = 0x04
        CMD_INCREMENT = 0x05
        CMD_DECREMENT = 0x06
        CMD_QUIT = 0x07
        CMD_FLUSH = 0x08
        CMD_GETQ = 0x09
        CMD_NOOP = 0x0a
        CMD_VERSION = 0x0b
        CMD_GETK = 0x0c
        CMD_GETKQ = 0x0d
        CMD_APPEND = 0x0e
        CMD_PREPEND = 0x0f
        CMD_STAT = 0x10
        CMD_SETQ = 0x11
        CMD_ADDQ = 0x12
        CMD_REPLACEQ = 0x13
        CMD_DELETEQ = 0x14
        CMD_INCREMENTQ = 0x15
        CMD_DECREMENTQ = 0x16
        CMD_QUITQ = 0x17
        CMD_FLUSHQ = 0x18
        CMD_APPENDQ = 0x19
        CMD_PREPENDQ = 0x1a
        CMD_VERBOSITY = 0x1b
        CMD_TOUCH = 0x1c
        CMD_GAT = 0x1d
        CMD_GATQ = 0x1e
        CMD_SASL_LIST_MECHS = 0x20
        CMD_SASL_AUTH = 0x21
        CMD_SASL_STEP = 0x22

        def self.build(opaque, vbucket, opcode, *args)
          case opcode
          when :set
            key, value, flags, expiration, cas = args.shift(5)
            bodylen = key.size + value.size + 8
            [
              0x80,             # uint8_t   magic
              CMD_SET,          # uint8_t   opcode
              key.size,         # uint16_t  keylen
              8,                # uint8_t   extlen (flags + expiration)
              0,                # uint8_t   datatype
              vbucket,          # uint16_t  vbucket
              bodylen,          # uint32_t  bodylen
              opaque || 0,      # uint32_t  opaque
              cas || 0,         # uint64_t  cas
              flags || 0,       # uint32_t  flags
              expiration || 0,  # uint32_t  expiration
              key,
              value
            ].pack("#{REQUEST_HEADER_FMT}NNa*a*")
          when :get
            key = args.shift
            bodylen = key.size
            [
              0x80,             # uint8_t   magic
              CMD_GET,          # uint8_t   opcode
              key.size,         # uint16_t  keylen
              0,                # uint8_t   extlen
              0,                # uint8_t   datatype
              vbucket,          # uint16_t  vbucket
              bodylen,          # uint32_t  bodylen
              opaque || 0,      # uint32_t  opaque
              0,                # uint64_t  cas
              key
            ].pack("#{REQUEST_HEADER_FMT}a*")
          when :sasl_auth
            mech, username, password = args.shift(3)
            value = "\0#{username}\0#{password}"
            bodylen = mech.size + value.size
            [
              0x80,             # uint8_t   magic
              CMD_SASL_AUTH,    # uint8_t   opcode
              mech.size,        # uint16_t  keylen
              0,                # uint8_t   extlen
              0,                # uint8_t   datatype
              0,                # uint16_t  vbucket
              bodylen,          # uint32_t  bodylen
              0,                # uint32_t  opaque
              0,                # uint64_t  cas
              mech,
              value
            ].pack("#{REQUEST_HEADER_FMT}a*a*")
          else
            raise Couchbase::Error::UnknownCommand, [opcode, *args].inspect
          end
        end

        def self.parse(data)
          while data.length >= RESPONSE_HEADER_SIZE
            header = data[0...RESPONSE_HEADER_SIZE]
            ( magic,
              opcode,
              keylen,
              extlen,
              datatype,
              status,
              bodylen,
              opaque,
              cas ) = header.unpack(RESPONSE_HEADER_FMT)

            if magic != 0x81
              fail Couchbase::Error::Protocol.new "Broken packet: #{header.inspect}"
            end

            if data.size < bodylen + RESPONSE_HEADER_SIZE
              return  # need moar data
            else
              data[0...RESPONSE_HEADER_SIZE] = ""
            end

            ext = data[0...extlen]
            data[0...extlen] = ""

            key = data[0...keylen]
            data[0...keylen] = ""

            vallen = bodylen - extlen - keylen
            body = data[0...vallen]
            data[0...vallen] = ""

            result = Result.new(:key => key,
                                :value => body,
                                :status => status,
                                :cas => cas)

            case opcode
            when CMD_SET
              result.operation = :set
            when CMD_SASL_AUTH
              result.operation = :sasl_auth
            when CMD_GET
              result.operation = :get
              result.flags, _ = ext.unpack("N")
            else
              raise Couchbase::Error::UnknownCommand, header.inspect
            end

            if error_class = Couchbase::Error.map_error_code(status)
              result.error = error_class.new(body)
              result.error.error = status
              result.error.key = key
              result.error.cas = cas
              result.error.operation = result.operation
            end
            yield(result.operation, opaque, result)
          end
        end
      end

    end

  end
end

