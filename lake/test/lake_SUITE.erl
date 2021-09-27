-module(lake_SUITE).

-export([all/0]).

-export([
    connect_and_close/1,
    connect_incorrect_credentials/1,
    connect_incorrect_vhost/1,
    subscribe_and_publish/1,
    credit/1,
    credit_wrong_subscription_id/1,
    query_publisher_sequence/1,
    query_publisher_sequence_errors/1,
    store_and_query_offset/1,
    metadata/1
]).

-include("response_codes.hrl").

all() ->
    [
        connect_and_close,
        connect_incorrect_credentials,
        connect_incorrect_vhost,
        subscribe_and_publish,
        credit,
        credit_wrong_subscription_id,
        query_publisher_sequence,
        query_publisher_sequence_errors,
        store_and_query_offset,
        metadata
    ].

host() -> {172, 17, 0, 2}.
port() -> 5552.
stream() -> <<"test-stream">>.

connect_and_close(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    ok = lake:stop(Connection),
    ok.

connect_incorrect_credentials(_Config) ->
    {error, _} = lake:connect(host(), port(), <<"does not exit">>, <<"guest">>, <<"/">>),
    {error, _} = lake:connect(host(), port(), <<"guest">>, <<"wrong_password">>, <<"/">>).

connect_incorrect_vhost(_Config) ->
    {error, _} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"does not exist">>).

subscribe_and_publish(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    Stream = stream(),
    ok = lake:create(Connection, Stream, []),
    SubscriptionId = 1,
    ok = lake:subscribe(Connection, Stream, SubscriptionId, first, 10, [
        {<<"some">>, <<"property">>}
    ]),
    PublisherId = 1,
    PublisherReference = <<"my-publisher">>,
    ok = lake:declare_publisher(Connection, Stream, PublisherId, PublisherReference),
    Message = <<"Hello, world!">>,
    {ok, {PublisherId, [1]}} = lake:publish_sync(Connection, PublisherId, [{1, Message}]),
    {ok, {[Message], Info}} =
        receive
            {deliver, 1, OsirisChunk} ->
                lake:chunk_to_messages(OsirisChunk)
        after 5000 ->
            exit(timeout)
        end,
    #{
        chunk_id := 0, number_of_entries := 1, number_of_records := 1
    } = Info,
    ok = lake:unsubscribe(Connection, SubscriptionId),
    ok = lake:delete_publisher(Connection, SubscriptionId),
    ok = lake:delete(Connection, Stream),
    ok = lake:stop(Connection),
    ok.

credit(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    Stream = stream(),
    ok = lake:create(Connection, Stream, []),
    SubscriptionId = 1,
    InitialCredits = 0,
    ok = lake:subscribe(Connection, Stream, SubscriptionId, first, InitialCredits, []),
    PublisherId = 1,
    ok = lake:declare_publisher(Connection, Stream, PublisherId, <<"my-publisher">>),
    {ok, {PublisherId, [1]}} = lake:publish_sync(Connection, PublisherId, [{1, <<"Hello, World">>}]),
    receive
        {deliver, 1, _} ->
            throw(unexpected)
    after 1000 ->
        ok
    end,
    ok = lake:credit_async(Connection, SubscriptionId, 1),
    {ok, {[_], _}} =
        receive
            {deliver, 1, OsirisChunk} ->
                lake:chunk_to_messages(OsirisChunk)
        after 5000 ->
            exit(timeout)
        end,
    {ok, {PublisherId, [1]}} = lake:publish_sync(Connection, PublisherId, [{1, <<"Hello, World">>}]),
    receive
        {deliver, 1, _} ->
            throw(unexpected)
    after 1000 ->
        ok
    end,
    ok = lake:unsubscribe(Connection, SubscriptionId),
    ok = lake:delete_publisher(Connection, SubscriptionId),
    ok = lake:delete(Connection, Stream),

    ok = lake:stop(Connection),
    ok.

credit_wrong_subscription_id(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    {error, _} = lake:credit_async(Connection, 1000, 1).

query_publisher_sequence(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    Stream = stream(),
    ok = lake:create(Connection, Stream, []),
    PublisherId = 0,
    PublisherReference = <<"my-publisher">>,
    ok = lake:declare_publisher(Connection, Stream, PublisherId, PublisherReference),
    {ok, 0} = lake:query_publisher_sequence(Connection, PublisherReference, Stream),
    Message = <<"Hello, world!">>,
    {ok, _} = lake:publish_sync(Connection, PublisherId, [{1, Message}]),
    {ok, 1} = lake:query_publisher_sequence(Connection, PublisherReference, Stream),
    {ok, _} = lake:publish_sync(Connection, PublisherId, [{2, Message}]),
    {ok, 2} = lake:query_publisher_sequence(Connection, PublisherReference, Stream),
    ok = lake:delete_publisher(Connection, PublisherId),
    ok = lake:delete(Connection, Stream),
    ok = lake:stop(Connection),
    ok.

query_publisher_sequence_errors(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    Stream = stream(),
    ok = lake:create(Connection, Stream, []),
    PublisherId = 0,
    PublisherReference = <<"my-publisher">>,
    ok = lake:declare_publisher(Connection, Stream, PublisherId, PublisherReference),
    {error, _} = lake:query_publisher_sequence(Connection, PublisherReference, <<"wrong stream">>).

store_and_query_offset(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    Stream = stream(),
    ok = lake:create(Connection, Stream, []),
    PublisherId = 0,
    PublisherReference = <<"my-publisher">>,
    ok = lake:declare_publisher(Connection, Stream, PublisherId, PublisherReference),
    {ok, 0} = lake:query_offset(Connection, PublisherReference, Stream),
    MessagesWithIds = [{Id, <<"Hello">>} || Id <- lists:seq(1, 10)],
    {ok, _} = lake:publish_sync(Connection, PublisherId, MessagesWithIds),
    {ok, 0} = lake:query_offset(Connection, PublisherReference, Stream),
    ok = lake:store_offset(Connection, PublisherReference, Stream, 5),
    {ok, 5} = lake:query_offset(Connection, PublisherReference, Stream),
    ok = lake:delete_publisher(Connection, PublisherId),
    ok = lake:delete(Connection, Stream),
    ok = lake:stop(Connection),
    ok.

metadata(_Config) ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    Stream = stream(),
    ok = lake:create(Connection, Stream, []),
    {ok, _Endpoints, _Metadata} = lake:metadata(Connection, [Stream, <<"does not exist">>]),
    ok = lake:delete(Connection, Stream),
    ok = lake:stop(Connection),
    ok.

%% FIXME test: frame size with large messages
%% FIXME test: after unsubscribe, no more messages are delivered
%% FIXME test: PublishError vs PublishConfirm; do we need to translate error codes?
%% FIXME test: mismatching CRC for chunk_to_messages
%% FIXME test: behaviour if connection is stopped while request is pending
%% FIXME test: QueryPublisherSequence, Credit, StoreOffset,QueryOffset, ...
%% FIXME test: delete publisher if publisher was not declared
%% FIXME test: unsubscribe without prior subscription
%% FIXME test: Connection stops if it sent an invalid command (Server sends CLOSE in that case)
