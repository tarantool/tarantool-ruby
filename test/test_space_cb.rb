require File.expand_path('../helper.rb', __FILE__)

describe EM::Tarantool::SpaceCB do
  let(:tarantool) { EM::Tarantool.new(TCONFIG[:host], TCONFIG[:port]) }
  let(:clear_space) { tarantool.space_cp(0) }
  let(:space0) { tarantool.space_cp(0, SPACE0[:types], pk: SPACE0[:pk], indexes: SPACE0[:indexes])}
  let(:space1) { tarantool.space_cp(1, SPACE1[:types], pk: SPACE1[:pk], indexes: SPACE1[:indexes])}
  let(:space2) { tarantool.space_cp(2, SPACE2[:types], pk: SPACE2[:pk], indexes: SPACE2[:indexes])}

  it "should be got from tarantool" do
    space0 = tarantool.space_cp(0)
    space0.must_be_kind_of EM::Tarantool::SpaceCB
  end

  it "should be queryable even without description" do
    result = nil
    emrun { clear_space.select(0, 0, -1, 'vasya'){|res| result = res; EM.stop} }
    result.must_equal [%W{vasya petrov eb@lo.com \x05\x00\x00\x00}]
  end

end
