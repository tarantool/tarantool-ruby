require 'fiber'

class Fiber
  alias call resume
end

module Kernel
  def fiber_yield
    Fiber.yield
  end
  alias fyield fiber_yield
end
