-module(benchmark).

-export([main/1, stream/0, max_rate/0]).

main(Args) ->
    [URI, User, Password, Vhost] = lists:map(fun normalize_string/1, Args),
    #{scheme := <<"streams">>, host := Host, port := Port} = uri_string:parse(URI),
    {ok, Connection} = lake:connect(Host, Port, User, Password, Vhost),
    %% Delete the stream if it already exists
    lake:delete(Connection, stream()),
    ok = lake:create(Connection, stream(), [{<<"max-age">>, <<"1h">>}]),

    io:format("Running benchmark. Hit enter to stop~n"),

    %% We start with 10 msgs/s
    PublishRate = atomics:new(1, [{signed, false}]),
    atomics:put(PublishRate, 1, 10),

    {ok, Collector} = benchmark_collector:start_link(),
    {ok, Subscriber} = benchmark_subscriber:start_link([
        {rabbitmq, [Host, Port, User, Password, Vhost]},
        {rate, PublishRate},
        {initial_credits, 100}
    ]),
    {ok, Publisher} = benchmark_publisher:start_link([
        {rabbitmq, [Host, Port, User, Password, Vhost]},
        {rate, PublishRate},
        {message, list_to_binary(lists:duplicate(350, $a))},
        {chunk_size, 1000}
    ]),

    io:get_line(""),

    benchmark_subscriber:stop(Subscriber),
    benchmark_publisher:stop(Publisher),
    benchmark_collector:stop(Collector),

    ok = lake:delete(Connection, stream()),
    ok = lake:stop(Connection),
    erlang:halt(0).

normalize_string(List) when is_list(List) ->
    list_to_binary(List);
normalize_string(Bin) when is_binary(Bin) ->
    Bin.

stream() -> <<"benchmark-lake">>.

max_rate() -> 1000000.
