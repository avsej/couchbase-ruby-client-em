# EXAMPLE

Like any Deferrable eventmachine-based protocol implementation, using EM-Couchbase involves making calls and passing blocks that serve as callbacks when the call returns.

    require 'em-couchbase'

    EM.run do
      couchbase = EM::Protocols::Couchbase.connect
      couchbase.set "a", "foo" do |response|
        if response.success?
          couchbase.get "a" do |response|
            puts response.inspect
          end
        end
      end
    end

