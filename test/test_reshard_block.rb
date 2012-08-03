require File.expand_path('../shared_reshard.rb', __FILE__)

describe "Resharding with blocking connection" do
  let(:tarantool_type){ :block }
  def bsleep(sec) sleep(sec) end
  it_behaves_like 'resharding'
end
