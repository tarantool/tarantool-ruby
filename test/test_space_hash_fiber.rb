require File.expand_path('../shared_space_hash.rb', __FILE__)

describe 'Tarantool::FiberDB::SpaceHash' do
  before { TConf.run(:master1) }
  let(:tarantool) { Tarantool.new(TCONFIG.merge(type: :em)) }
  alias blockrun fibrun
  it_behaves_like :blocking_hash_space
end
