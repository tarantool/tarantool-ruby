require File.expand_path('../shared_replicated_shard.rb', __FILE__)

describe "Replication Shard with blocking connection" do
  let(:tarantool_type){ :block }
  def bsleep(sec) sleep(sec) end
  it_behaves_like 'replication and shards'
end
