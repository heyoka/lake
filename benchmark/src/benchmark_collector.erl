-module(benchmark_collector).

-export([start_link/0, stop/1, collect/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_continue/2]).

start_link() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

stop(Collector) ->
    gen_server:stop(Collector).

collect(Read = {read, _}) ->
    gen_server:cast(?MODULE, Read);
collect(Wrote = {wrote, _}) ->
    gen_server:cast(?MODULE, Wrote).

-record(state, {read_table = #{}, wrote_table = #{}, start_ts = erlang:monotonic_time(seconds)}).

init([]) ->
    {ok, #state{}, {continue, no_data}}.

handle_continue(no_data, State) ->
    timer:sleep(2000),
    {ok, _} = timer:send_interval(1000, output_collected_stats),
    {noreply, State}.

handle_call(Unknown, _, State) ->
    {stop, {error, Unknown}, State}.

handle_cast({read, Stats}, State) ->
    {noreply, add_to_table(read, Stats, State)};
handle_cast({wrote, Stats}, State) ->
    {noreply, add_to_table(wrote, Stats, State)}.

handle_info(output_collected_stats, State) ->
    OutputTime = erlang:monotonic_time(seconds) - 1,
    print(OutputTime, State),
    {noreply, remove_from_tables(OutputTime, State)}.

previous_full_second(TimestampMs) ->
    TimestampMs div 1000.

add_to_table(read, {TimestampMs, Read}, State) ->
    ReadTable = State#state.read_table,
    State#state{read_table = ReadTable#{previous_full_second(TimestampMs) => Read}};
add_to_table(wrote, {TimestampMs, Read}, State) ->
    ReadTable = State#state.wrote_table,
    State#state{wrote_table = ReadTable#{previous_full_second(TimestampMs) => Read}}.

remove_from_tables(TimestampS, State) ->
    State#state{
        read_table = maps:remove(TimestampS, State#state.read_table),
        wrote_table = maps:remove(TimestampS, State#state.wrote_table)
    }.

print(TimestampS, State) ->
    DiffToStart = TimestampS - State#state.start_ts,
    {MessagesRead, TimeForReads, MeasuredReadRate} = maps:get(
        TimestampS, State#state.read_table, unknown
    ),
    {MessagesWritten, TimeForWrites, MeasuredWriteRate} = maps:get(
        TimestampS, State#state.wrote_table, unknown
    ),
    io:format(
        "~p: wrote ~p msgs in ~ps (~p msgs/s), read ~p msgs with ~ps remaining to full second (~p msgs/s)~n",
        [
            DiffToStart,
            MessagesWritten,
            TimeForWrites,
            trunc(MeasuredWriteRate),
            MessagesRead,
            TimeForReads,
            MeasuredReadRate
        ]
    ).
