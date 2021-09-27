defmodule LakeHouseTest do
  use ExUnit.Case
  doctest LakeHouse

  setup do
    [
      host: System.get_env("RABBITMQ_HOST") || throw("RABBITMQ_HOST not set"),
      user: "guest",
      password: "guest",
      vhost: "/"
    ]
  end

  test "subscribe and publish", ctx do
    stream = "my-stream"
    subscription_id = 1
    publisher_id = 1
    publisher_reference = "my-publisher"

    assert {:ok, connection} = LakeHouse.connect(ctx.host, ctx.user, ctx.password, ctx.vhost)
    assert :ok == LakeHouse.create(connection, stream)
    assert :ok == LakeHouse.subscribe(connection, stream, subscription_id, :first, 10)

    assert :ok ==
             LakeHouse.declare_publisher(connection, stream, publisher_id, publisher_reference)

    assert {:ok, {publisher_id, [1]}} ==
             LakeHouse.publish_sync(connection, publisher_id, [{1, "Hello, World!"}])

    {:ok, delivered_and_parsed} =
      receive do
        {:deliver, 1, osiris_chunk} ->
          LakeHouse.chunk_to_messages(osiris_chunk)
      after
        1000 ->
          exit(:timeout)
      end

    assert {["Hello, World!"], info} = delivered_and_parsed
    assert %{chunk_id: 0, number_of_entries: 1, number_of_records: 1} = info

    assert :ok == LakeHouse.unsubscribe(connection, subscription_id)
    assert :ok == LakeHouse.delete_publisher(connection, publisher_id)
    assert :ok == LakeHouse.delete(connection, stream)
    assert :ok == LakeHouse.stop(connection)
  end

  test "metadata", ctx do
    stream = "my-stream"
    assert {:ok, connection} = LakeHouse.connect(ctx.host, ctx.user, ctx.password, ctx.vhost)
    assert :ok == LakeHouse.create(connection, stream)
    assert {:ok, _endpoints, _replicas} = LakeHouse.metadata(connection, [stream])
    assert :ok == LakeHouse.delete(connection, stream)
    assert :ok == LakeHouse.stop(connection)
  end

  test "store and query offset", ctx do
    stream = "my-stream"
    publisher_id = 1
    publisher_reference = "my-publisher"

    assert {:ok, connection} = LakeHouse.connect(ctx.host, ctx.user, ctx.password, ctx.vhost)
    assert :ok == LakeHouse.create(connection, stream)

    assert :ok ==
             LakeHouse.declare_publisher(connection, stream, publisher_id, publisher_reference)

    assert {:ok, 0} == LakeHouse.query_offset(connection, publisher_reference, stream)

    assert {:ok, {publisher_id, [1]}} ==
             LakeHouse.publish_sync(connection, publisher_id, [{1, "Hello, World!"}])

    assert :ok == LakeHouse.store_offset(connection, publisher_reference, stream, 1)
    assert {:ok, 1} == LakeHouse.query_offset(connection, publisher_reference, stream)
    assert :ok == LakeHouse.delete_publisher(connection, publisher_id)
    assert :ok == LakeHouse.delete(connection, stream)
    assert :ok == LakeHouse.stop(connection)
  end
end
