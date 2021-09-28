-module(lake_heartbeat).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start_link(Heartbeat, Connection) ->
    gen_server:start_link(?MODULE, [Heartbeat, Connection], []).

init([Heartbeat, Connection]) ->
    {ok, _} = timer:send_interval(Heartbeat * 1000, check_heartbeat),
    ok = gen_server:cast(Connection, heartbeat),
    {ok, #{heartbeat => Heartbeat, connection => Connection, last_heartbeat => undefined}}.

handle_call(_, _From, State) ->
    {reply, unknown, State}.

handle_cast({heartbeat}, State) ->
    {noreply, State#{last_heartbeat => erlang:monotonic_time(second)}}.

handle_info(
    check_heartbeat,
    State = #{heartbeat := Heartbeat, last_heartbeat := LastHeartbeat, connection := Connection}
) ->
    Now = erlang:monotonic_time(second),
    if
        Now - 2 * Heartbeat > LastHeartbeat ->
            {stop, heartbeat_timeout, State};
        true ->
            ok = gen_server:cast(Connection, heartbeat),
            {noreply, State}
    end.
