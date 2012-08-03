require File.expand_path('../shared_reshard.rb', __FILE__)

describe "Resharding with fibered connection" do
  let(:tarantool_type){ :em_fiber }
  alias blockrun fibrun
  def bsleep(sec)
    EM.add_timer(sec, Fiber.current)
    Fiber.yield
  end
  it_behaves_like 'resharding'
end
