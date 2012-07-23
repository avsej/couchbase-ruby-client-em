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

      class Node < EventMachine::Connection
        include EM::Deferrable

        attr_reader :client

        attr_reader :proxy
        attr_reader :direct
        attr_reader :couch
        attr_reader :admin
        attr_reader :status
        attr_reader :version

        def initialize(options = {})
          @data = ""  # naive buffer implementation
          options.each do |k, v|
            if respond_to?(k)
              instance_variable_set("@#{k}", v)
            end
          end
        end

        def self.connect(options)
          host, port = options[:direct].split(':')
          EventMachine.connect(host, port, self, options)
        end

        def receive_data(data)
          @data << data
          Packet.parse(@data) do |op, opaque, result|
            if result.error.class == Error::NotMyVbucket
              client.retry(:not_my_vbucket, opaque)
            else
              if op == :sasl_auth
                raise result.error unless result.success?
              else
                client.run_callback(opaque, result)
              end
            end
          end
        end

        def set(opaque, vbucket, key, value, options = {})
          packet = Packet.build(opaque, vbucket, :set,
                                key, value,
                                options[:flags],
                                options[:expiration],
                                options[:cas])
          client.register_packet(opaque, packet)
          send_data(packet)
        end

        def arithm(opcode, opaque, vbucket, key, options = {})
          if options.is_a?(Fixnum)
            options = {:delta => options}
          end
          packet = Packet.build(opaque, vbucket,
                                opcode, key,
                                options[:delta],
                                options[:initial],
                                options[:expiration],
                                options[:cas])
          client.register_packet(opaque, packet)
          send_data(packet)
        end

        # @param opaque [Fixnum]
        # @param pairs [Array] array of tuples +[opaque, vbucket, key]+
        # @param options [Hash]
        def get(tuples, options = {})
          packets = ""
          tuples.each do |opaque, vbucket, key|
            packet = Packet.build(opaque, vbucket, :get, key)
            client.register_packet(opaque, packet)
            packets << packet
          end
          send_data(packets)
        end

        def ==(other)
          other = Node.new(other) if other.is_a?(Hash)
          self.class == other.class &&
            self.direct == other.direct &&
            self.couch == other.couch &&
            self.status == other.status
        end

        protected

        def authenticate
          if client.config.sasl_password
            # currently Couchbase supports PLAIN only authentication
            # this is why it doesn't ask list of mechanisms
            packet = Packet.build(0, 0, :sasl_auth, "PLAIN",
                                  client.config.bucket_name,
                                  client.config.sasl_password)
            send_data(packet)
          end
        end

        def connection_completed
          authenticate
          succeed
        end

      end
    end
  end
end
