require File.expand_path('../shared_replicated_shard.rb', __FILE__)

describe "Replication Shard with fibered connection" do
  let(:tarantool_type){ :em_fiber }
  alias blockrun fibrun
  it_behaves_like 'replication and shards'
end
