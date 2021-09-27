defmodule LakeHouse do
  @moduledoc """
  RabbitMQ Streams Connector.
  """

  def connect(host, port \\ 5552, user, password, vhost) do
    :lake.connect(String.to_charlist(host), port, user, password, vhost)
  end

  defdelegate stop(connection), to: :lake

  defdelegate declare_publisher(connection, stream, publisher_id, publisher_reference), to: :lake

  defdelegate publish_sync(connection, publisher_id, messages), to: :lake

  defdelegate create(connection, stream, properties \\ []), to: :lake

  defdelegate subscribe(
                connection,
                stream,
                subscription_id,
                offset_definition,
                offset,
                properties \\ []
              ),
              to: :lake

  defdelegate unsubscribe(connection, subscription_id), to: :lake

  defdelegate delete_publisher(connection, publisher_id), to: :lake

  defdelegate delete(connection, stream), to: :lake

  defdelegate chunk_to_messages(osiris_chunk), to: :lake

  defdelegate store_offset(connection, publisher_reference, stream, offset), to: :lake

  defdelegate query_offset(connection, publisher_reference, stream), to: :lake

  defdelegate metadata(connection, stream), to: :lake
end
