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

      class Result
        attr_accessor :cas
        attr_accessor :error
        attr_accessor :flags
        attr_accessor :key
        attr_accessor :node
        attr_accessor :operation
        attr_accessor :value

        def initialize(attributes = {})
          attributes.each do |k, v|
            if respond_to?(k)
              instance_variable_set("@#{k}", v)
            end
          end
        end

        def success?
          !@error
        end

      end

    end
  end
end
