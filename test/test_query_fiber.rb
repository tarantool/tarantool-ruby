require File.expand_path('../shared_query.rb', __FILE__)

describe 'Tarantool::FiberDB::Query' do
  before { TConf.run(:master1) }
  let(:tarantool) { Tarantool.new(TCONFIG.merge(type: :em)) }
  alias blockrun fibrun
  it_behaves_like :blocking_query
end
