require File.expand_path('../shared_space_array.rb', __FILE__)

describe 'Tarantool::FiberDB::SpaceArray' do
  let(:tarantool) { Tarantool.new(TCONFIG.merge(type: :em)) }
  alias blockrun fibrun
  it_behaves_like :blocking_array_space
end
