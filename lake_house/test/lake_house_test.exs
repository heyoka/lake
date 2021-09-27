defmodule LakeHouseTest do
  use ExUnit.Case
  doctest LakeHouse

  test "greets the world" do
    assert LakeHouse.hello() == :world
  end
end
