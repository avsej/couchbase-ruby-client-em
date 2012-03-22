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

      class Client
        include EM::Deferrable

        attr_reader :nodes
        attr_reader :config

        def initialize
          @opaque = 0
          @nodes = []
          @packets = {}
          @upgrade_queue = EM::Queue.new
        end

        # @param options [Hash]
        # @option options [String] :hostname ("localhost")
        # @option options [Fixnum] :port (8091)
        # @option options [String] :pool ("default")
        # @option options [String] :bucket ("default")
        # @option options [String] :username (nil)
        # @option options [String] :password (nil)
        def connect(options = {})
          @config_listener = ConfigurationListener.new
          @config_listener.on_error do |listener, error|
            @on_error.call(self, error) if @on_error
          end
          @config_listener.on_upgrade do |config|
            @config = config
            config.nodes.each_with_index do |nn, ii|
              if nodes[ii] != nn
                nodes[ii].disconnect if nodes[ii]
                options = nn.merge(:client => self)
                nodes[ii] = Node.connect(options)
              end
            end
            do_retry = lambda do |packet|
              opaque, key, handler, raw = packet.values_at(:opaque, :key, :handler, :raw)
              register_handler(opaque, key, handler)
              vbucket, node = locate(key)
              raw[6...7] = [vbucket].pack("n")
              register_packet(opaque, raw)
              node.callback do
                node.send_data(raw)
              end
              @upgrade_queue.pop(&do_retry)
            end
            @upgrade_queue.pop(&do_retry)
            succeed
          end
          @config_listener.listen(options)
          self
        end

        def on_error(&callback)
          @on_error = callback
        end

        def register_packet(opaque, packet)
          if packet.respond_to?(:force_encoding)
            packet.force_encoding(Encoding::BINARY)
          end
          (@packets[opaque] ||= {})[:raw] = packet
        end

        def register_handler(opaque, key, handler)
          packet = (@packets[opaque] ||= {})
          packet[:key] = key
          packet[:handler] = handler
        end

        def retry(reason, opaque)
          packet = @packets.delete(opaque)
          if packet
            case reason
            when :not_my_vbucket
              @upgrade_queue.push([opaque, packet])
            end
          end
        end

        def run_callback(opaque, result)
          packet = @packets.delete(opaque)
          key, handler = packet.values_at(:key, :handler)
          if handler.respond_to?(:call)
            result.key = key
            handler.call(result)
          end
        end

        # Locate node using vbucket distribution
        # @return [Fixnum] server index
        def locate(key)
          digest = Zlib.crc32(key)
          mask = @config.vbucket_map.size - 1
          vbucket = digest & mask
          [vbucket, @nodes[@config.vbucket_map[vbucket][0]]]
        end

        def set(key, val, options = {}, &block)
          callback do
            opaque = opaque_inc
            register_handler(opaque, key, block)
            vbucket, node = locate(key)
            node.callback do
              node.set(opaque, vbucket, key, val, options)
            end
          end
        end

        def get(*keys, &block)
          callback do
            if keys.last.is_a?(Hash)
              options = keys.last.pop
            end
            groups = keys.inject({}) do |acc, key|
              opaque = opaque_inc
              register_handler(opaque, key, block)
              vbucket, node = locate(key)
              acc[node] ||= []
              acc[node] << [opaque, vbucket, key]
              acc
            end
            groups.each do |node, tuple|
              node.callback do
                node.get(tuple, options)
              end
            end
          end
        end

        protected

        def unbind
          nodes.each do |node|
            nodes.disconnect
          end
        end

        def opaque_inc
          @opaque += 1
          @opaque &= 0xffffffff # 32 bits
        end

      end
    end
  end
end
