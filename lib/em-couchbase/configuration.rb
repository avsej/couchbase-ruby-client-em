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

      class Configuration
        attr_reader :bucket_name
        attr_reader :bucket_type
        attr_reader :sasl_password
        attr_reader :node_locator
        attr_reader :nodes
        attr_reader :hash_algorithm
        attr_reader :num_replicas
        attr_reader :vbucket_map
        attr_reader :vbucket_map_forward

        def initialize(attributes = {})
          attributes.each do |k, v|
            if respond_to?(k)
              instance_variable_set("@#{k}", v)
            end
          end
        end
      end

      class ConfigurationListener
        attr_reader :config_stream
        attr_reader :options

        def initialize
          @parser = Yajl::Parser.new
          @parser.on_parse_complete = lambda do |object|
            if @on_upgrade
              config = build_config(object)
              if config
                @on_upgrade.call(config)
              end
            end
          end
        end

        def on_upgrade(&callback)
          @on_upgrade = callback
        end

        def on_error(&callback)
          @on_error = callback
          if @config_stream
            @config_stream.errback(&callback)
          end
        end

        def listen(options = {})
          @options = {
            :hostname => "localhost",
            :port => 8091,
            :pool => "default",
            :bucket => "default"
          }.merge(options)
          params = {:head => {}}
          (username, password) = auth = @options.values_at(:username, :password)
          if username && password
            params[:head][:authorization] = auth
          end
          uri = sprintf("http://%s:%d/pools/%s/bucketsStreaming/%s/",
                        *@options.values_at(:hostname, :port, :pool, :bucket))
          @config_stream = EM::HttpRequest.new(URI.parse(uri),
                                               :inactivity_timeout => 0).get params
          @config_stream.errback do |http|
            @on_error.call(self, http.error) if @on_error && http.error
          end
          @config_stream.stream do |chunk|
            @parser << chunk
          end
          @config_stream
        end

        def build_config(json)
          return unless json.is_a?(Hash)

          bucket_name = json.fetch("name")
          bucket_type = json.fetch("bucketType")
          sasl_password = json.fetch("saslPassword")
          node_locator = json.fetch("nodeLocator")
          if node_locator == "vbucket"
            server_map = json.fetch("vBucketServerMap")
            num_replicas = server_map.fetch("numReplicas")
            hash_algorithm = server_map.fetch("hashAlgorithm")
            vbucket_map = server_map.fetch("vBucketMap")
            vbucket_map_forward = server_map["vBucketMapForward"]
          end
          nodes = json.fetch("nodes")
          if nodes.empty?
            raise ArgumentError, "empty list of nodes"
          end
          nodes = nodes.map do |node|
            admin = node.fetch("hostname")
            ports = node.fetch("ports")
            {
              :admin => admin,
              :proxy => admin.sub(/:\d+$/, ":#{ports.fetch("proxy")}"),
              :direct => admin.sub(/:\d+$/, ":#{ports.fetch("direct")}"),
              :couch => node["couchApiBase"], # nil for 1.8.x series
              :status => node.fetch("status"),
              :version => node.fetch("version")
            }
          end.sort_by{|node| node[:direct]}
          Configuration.new(:bucket_name => bucket_name,
                            :bucket_type => bucket_type,
                            :sasl_password => sasl_password,
                            :node_locator => node_locator,
                            :num_replicas => num_replicas,
                            :hash_algorithm => hash_algorithm,
                            :vbucket_map => vbucket_map,
                            :vbucket_map_forward => vbucket_map_forward,
                            :nodes => nodes)
        end
      end

    end
  end
end
