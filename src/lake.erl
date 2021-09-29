%%
%% @doc Connect to and use RabbitMQ Streams.
%%
-module(lake).

-export([tls_connect/4, tls_connect/5, connect/4, connect/5, connect/6, stop/1]).

-export([
    declare_publisher/4,
    publish_sync/3,
    query_publisher_sequence/3,
    delete_publisher/2,
    credit_async/3,
    create/3,
    delete/2,
    subscribe/6,
    store_offset/4,
    query_offset/3,
    unsubscribe/2,
    metadata/2
]).

-export([chunk_to_messages/1]).

%% @doc not yet implemented
tls_connect(Host, User, Password, Vhost) ->
    tls_connect(Host, 5551, User, Password, Vhost).

%% @doc not yet implemented
tls_connect(_Host, _Port, _User, _Password, _Vhost) ->
    throw(not_implemented).

%% @doc Establish a connection.
%% @see connect/5
%% @see connect/6
connect(Host, User, Password, Vhost) ->
    connect(Host, 5552, User, Password, Vhost).

%%
%% @doc Establish a connection.
%% @see connect/4
%% @see connect/6
connect(Host, Port, User, Password, Vhost) ->
    connect(Host, Port, User, Password, Vhost, []).

%%
%% @doc Establish a connection.
%% @see connect/4
%% @see connect/5
connect(Host, Port, User, Password, Vhost, Options) ->
    lake_connection:connect(lake_utils:normalize_host(Host), Port, User, Password, Vhost, Options).

%% @doc Stop a connection.
stop(Connection) ->
    lake_connection:stop(Connection).

%%
%% @doc Declare a publisher.
%%
declare_publisher(Connection, Stream, PublisherId, PublisherReference) when
    is_binary(Stream), is_integer(PublisherId), is_binary(PublisherReference)
->
    lake_connection:declare_publisher(Connection, Stream, PublisherId, PublisherReference).

%%
%% @doc Publish a message synchronously.
%%
publish_sync(Connection, PublisherId, Messages) when is_integer(PublisherId), is_list(Messages) ->
    lake_connection:publish_sync(Connection, PublisherId, Messages).

%%
%% @doc Query a publisher's sequence.
%%
query_publisher_sequence(Connection, PublisherReference, Stream) when
    is_binary(PublisherReference), is_binary(Stream)
->
    lake_connection:query_publisher_sequence(Connection, PublisherReference, Stream).

%%
%% @doc Delete a publisher.
%%
delete_publisher(Connection, PublisherId) when is_integer(PublisherId) ->
    lake_connection:delete_publisher(Connection, PublisherId).

%%
%% @doc Set a subscription's credit asynchronously.
%%
credit_async(Connection, SubscriptionId, Credit) when
    is_integer(SubscriptionId), is_integer(Credit)
->
    lake_connection:credit_async(Connection, SubscriptionId, Credit).

%%
%% @doc Create a new stream.
%%
create(Connection, Stream, Arguments) when is_binary(Stream), is_list(Arguments) ->
    lake_connection:create(Connection, Stream, Arguments).

%%
%% @doc Delete a stream.
%%
delete(Connection, Stream) when is_binary(Stream) ->
    lake_connection:delete(Connection, Stream).

%%
%% @doc Subscribe to a stream.
%%
subscribe(Connection, Stream, SubscriptionId, OffsetDefinition, Credit, Properties) when
    is_binary(Stream),
    is_integer(SubscriptionId),
    (is_tuple(OffsetDefinition) or is_atom(OffsetDefinition)),
    is_integer(Credit),
    is_list(Properties)
->
    lake_connection:subscribe(
        Connection, Stream, SubscriptionId, OffsetDefinition, Credit, Properties
    ).

%%
%% @doc Unsubscribe from a stream.
%%
unsubscribe(Connection, SubscriptionId) when is_integer(SubscriptionId) ->
    lake_connection:unsubscribe(Connection, SubscriptionId).

%%
%% @doc Store a publisher's offset to the stream.
%%
%% FIXME store_offset/4 is fire-and-forget; maybe the name should indicate that?
%%
store_offset(Connection, PublisherReference, Stream, Offset) when
    is_binary(PublisherReference), is_binary(Stream), is_integer(Offset)
->
    lake_connection:store_offset(Connection, PublisherReference, Stream, Offset).

%%
%% @doc Query a publisher's stored offset from the stream.
%%
query_offset(Connection, PublisherReference, Stream) when
    is_binary(PublisherReference), is_binary(Stream)
->
    lake_connection:query_offset(Connection, PublisherReference, Stream).

%%
%% @doc Retrieve stream metadata such as endpoints and replicas.
%%
metadata(Connection, Streams) when is_list(Streams) ->
    lake_connection:metadata(Connection, Streams).

%%
%% @doc Convert a delivered osiris chunk into a list of messages.
%%
chunk_to_messages(OsirisChunk) ->
    lake_messages:chunk_to_messages(OsirisChunk).
