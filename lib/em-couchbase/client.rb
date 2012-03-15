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
        attr_reader :uri
        attr_reader :config

        def initialize
          @opaque = 0
          @nodes = []
          @handlers = {}
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
          @config_listener.on_upgrade do |config|
            @config = config
            config.nodes.each_with_index do |nn, ii|
              if nodes[ii] != nn
                nodes[ii].disconnect if nodes[ii]
                options = nn.merge(:client => self)
                nodes[ii] = Node.connect(options)
              end
            end
            succeed
          end
          @config_listener.listen(options)
          self
        end

        def run_callback(opaque, result)
          key, handler = @handlers.delete(opaque)
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
            @handlers[opaque] = [key, block]
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
              @handlers[opaque] = [key, block]
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
