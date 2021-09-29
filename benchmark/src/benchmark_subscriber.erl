-module(benchmark_subscriber).
-behaviour(gen_server).

-export([start_link/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
    connection,
    rate,
    message_count = 0,
    last_measurement_at = erlang:monotonic_time(milli_seconds),
    last_message_at = erlang:monotonic_time(milli_seconds)
}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

stop(Subscriber) ->
    ok = gen_server:call(Subscriber, stop),
    gen_server:stop(Subscriber).

init(Proplist) ->
    [Host, Port, User, Password, Vhost] = proplists:get_value(rabbitmq, Proplist),
    Rate = proplists:get_value(rate, Proplist),
    InitialCredits = proplists:get_value(initial_credits, Proplist),
    {ok, Connection} = lake:connect(Host, Port, User, Password, Vhost),
    {ok, _} = timer:send_interval(1000, measure_throughput),
    ok = lake:subscribe(
        Connection, benchmark:stream(), subscription_id(), first, InitialCredits, []
    ),
    State = #state{connection = Connection, rate = Rate},
    {ok, State}.

handle_call(stop, _from, State) ->
    {reply, lake:unsubscribe(State#state.connection, subscription_id()), State}.

handle_cast(Other, State) ->
    {stop, {error, {other, Other}}, State}.

handle_info(measure_throughput, State) ->
    #state{
        connection = Connection,
        last_measurement_at = LastMeasurementAt,
        last_message_at = LastMessageAt,
        message_count = MessageCount,
        rate = Rate0
    } = State,
    Now = erlang:monotonic_time(milli_seconds),

    Rate1 = atomics:get(Rate0, 1),
    Diff_ms = Now - LastMessageAt,
    Rate2 = new_rate(Rate1, Diff_ms),
    MeasuredRate_s = MessageCount / ((Now - LastMeasurementAt) / 1000),
    benchmark_collector:collect({read, {Now, {MessageCount, Diff_ms / 1000, MeasuredRate_s}}}),
    lake:credit_async(Connection, subscription_id(), 10 * Rate2),
    atomics:put(Rate0, 1, Rate2),
    {noreply, State#state{message_count = 0, last_measurement_at = Now}};
handle_info({deliver, 1, OsirisChunk}, State) ->
    Now = erlang:monotonic_time(milli_seconds),
    {ok, {_, #{number_of_entries := Count}}} = lake:chunk_to_messages(OsirisChunk),
    {noreply, State#state{
        message_count = State#state.message_count + Count,
        last_message_at = Now
    }}.

new_rate(CurrentRate, Diff_ms) ->
    %% If the last message arrived a long time ago, the publisher can publish a lot more messages.
    %% If the last message arrived just yet, any component here is overwhelmed.
    if
        Diff_ms > 750 ->
            CurrentRate * 4;
        Diff_ms > 500 ->
            CurrentRate * 2;
        Diff_ms > 250 ->
            (CurrentRate * 3) div 2;
        Diff_ms > 0 ->
            CurrentRate;
        true ->
            CurrentRate div 2
    end.

subscription_id() -> 1.
