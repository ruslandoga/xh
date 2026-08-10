defmodule XhTest do
  use ExUnit.Case
  doctest Xh

  test "greets the world" do
    assert Xh.hello() == :world
  end
end
