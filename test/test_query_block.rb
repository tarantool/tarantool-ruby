require File.expand_path('../shared_space_array.rb', __FILE__)

describe 'Tarantool::FiberDB::SpaceArray' do
  before { TConf.run(:master1) }
  let(:tarantool) { Tarantool.new(TCONFIG.merge(type: :block)) }
  it_behaves_like :blocking_array_space
end
