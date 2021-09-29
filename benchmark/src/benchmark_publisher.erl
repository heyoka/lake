-module(benchmark_publisher).
-behaviour(gen_server).

-export([start_link/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
    connection,
    publish_id = 0,
    rate,
    message,
    chunk_size
}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

stop(Publisher) ->
    ok = gen_server:call(Publisher, stop),
    gen_server:stop(Publisher).

init(Proplist) ->
    [Host, Port, User, Password, Vhost] = proplists:get_value(rabbitmq, Proplist),
    Rate = proplists:get_value(rate, Proplist),
    Message = proplists:get_value(message, Proplist),
    ChunkSize = proplists:get_value(chunk_size, Proplist),
    {ok, Connection} = lake:connect(Host, Port, User, Password, Vhost),
    ok = lake:declare_publisher(
        Connection, benchmark:stream(), publisher_id(), publisher_reference()
    ),
    {ok, _} = timer:send_interval(1000, send_messages),
    State = #state{connection = Connection, rate = Rate, message = Message, chunk_size = ChunkSize},
    {ok, State}.

handle_call(stop, _from, State) ->
    {reply, lake:delete_publisher(State#state.connection, publisher_id()), State}.

handle_cast(Other, State) ->
    {stop, {error, {other, Other}}, State}.

handle_info(send_messages, State = #state{}) ->
    #state{
        rate = Rate0,
        message = Message,
        publish_id = PublishId0,
        chunk_size = ChunkSize,
        connection = Connection
    } = State,
    {ok, Sequence} = lake:query_publisher_sequence(
        Connection, publisher_reference(), benchmark:stream()
    ),
    logger:debug("Publisher ~p messages in total", [Sequence]),

    Rate = atomics:get(Rate0, 1),
    {PublishId, Messages} = build_messages(Rate, PublishId0, Message, []),
    send_in_chunks(Connection, chunkify(Messages, ChunkSize)),
    {noreply, State#state{publish_id = PublishId}}.

build_messages(0, PublishId, _Message, Messages) ->
    {PublishId, Messages};
build_messages(Count, PublishId, Message, Messages) ->
    build_messages(Count - 1, PublishId + 1, Message, [{PublishId, Message} | Messages]).

send_in_chunks(_, []) ->
    ok;
send_in_chunks(Connection, [Chunk | Chunks]) ->
    _ = lake:publish_sync(Connection, publisher_id(), Chunk),
    send_in_chunks(Connection, Chunks).

%% Because lists:split/2 crashes..
chunkify(Messages, ChunkSize) ->
    chunkify(Messages, 0, ChunkSize, [[]]).

chunkify([], _, _, Acc) ->
    Acc;
chunkify([Message | Rest], RunSize, ChunkSize, [CurrentRun | Chunks]) when RunSize < ChunkSize ->
    chunkify(Rest, RunSize + 1, ChunkSize, [[Message | CurrentRun] | Chunks]);
chunkify([Message | Rest], _, ChunkSize, Acc) ->
    chunkify(Rest, 1, ChunkSize, [[Message] | Acc]).

publisher_id() -> 1.
publisher_reference() -> <<"benchmark-publisher">>.
