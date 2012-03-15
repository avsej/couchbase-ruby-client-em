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

      module Error

        # @return [Couchbase::Error::Base]
        def self.map_error_code(code)
          case code
          when 0x00   # PROTOCOL_BINARY_RESPONSE_SUCCESS
            nil
          when 0x01   # PROTOCOL_BINARY_RESPONSE_KEY_ENOENT
            NotFound
          when 0x02   # PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS
            KeyExists
          when 0x03   # PROTOCOL_BINARY_RESPONSE_E2BIG
            TooBig
          when 0x04   # PROTOCOL_BINARY_RESPONSE_EINVAL
            Invalid
          when 0x05   # PROTOCOL_BINARY_RESPONSE_NOT_STORED
            NotStored
          when 0x06   # PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL
            DeltaBadval
          when 0x07   # PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET
            NotMyVbucket
          when 0x20   # PROTOCOL_BINARY_RESPONSE_AUTH_ERROR
            Auth
          when 0x22   # PROTOCOL_BINARY_RESPONSE_ERANGE
            Range
          when 0x81   # PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND
            UnknownCommand
          when 0x82   # PROTOCOL_BINARY_RESPONSE_ENOMEM
            NoMemory
          when 0x83   # PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED
            NotSupported
          when 0x84   # PROTOCOL_BINARY_RESPONSE_EINTERNAL
            Internal
          when 0x85   # PROTOCOL_BINARY_RESPONSE_EBUSY
            Busy
          else
            Base
          end
        end

        class Base < ::RuntimeError
          # @return [Fixnum] the error code from libcouchbase
          attr_accessor :error
          # @return [String] the key which generated error */
          attr_accessor :key
          # @return [Fixnum] the version of the key (+nil+ unless accessible)
          attr_accessor :cas
          # @return [Symbol] the operation (+nil+ unless accessible)
          attr_accessor :operation
        end

        # Authentication error
        class Auth < Base; end

        # The given bucket not found in the cluster */
        class BucketNotFound < Base; end

        # The cluster is too busy now. Try again later */
        class Busy < Base; end

        # The given value is not a number */
        class DeltaBadval < Base; end

        # Internal error */
        class Internal < Base; end

        # Invalid arguments */
        class Invalid < Base; end

        # Key already exists */
        class KeyExists < Base; end

        # Network error */
        class Network < Base; end

        # Out of memory error */
        class NoMemory < Base; end

        # No such key */
        class NotFound < Base; end

        # The vbucket is not located on this server */
        class NotMyVbucket < Base; end

        # Not stored */
        class NotStored < Base; end

        # Not supported */
        class NotSupported < Base; end

        # Invalid range */
        class Range < Base; end

        # Temporary failure. Try again later */
        class TemporaryFail < Base; end

        # Object too big */
        class TooBig < Base; end

        # Unknown command */
        class UnknownCommand < Base; end

        # Unknown host */
        class UnknownHost < Base; end

        # Failed to decode or encode value */
        class ValueFormat < Base; end

        # Protocol error */
        class Protocol < Base; end

        # Timeout error */
        class Timeout < Base; end

        # Connect error */
        class Connect < Base; end
      end

    end
  end
end




