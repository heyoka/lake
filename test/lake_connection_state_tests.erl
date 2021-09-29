-module(lake_connection_state_tests).

-include_lib("eunit/include/eunit.hrl").
-include("lake_connection_state.hrl").

new_no_heartbeat_test() ->
    %% Heartbeat = 0 => No Heartbeat
    ?assertMatch(#state{heartbeat = undefined}, lake_connection_state:new(ignored, ignored, 0)).

new_with_heartbeat_test() ->
    %% Heartbeat =/= 0 => No Heartbeat
    State = lake_connection_state:new(ignored, ignored, 10),
    ?assert(is_process_alive(State#state.heartbeat)).

subscription_test() ->
    State0 = lake_connection_state:new(ignored, ignored, 0),
    State1 = lake_connection_state:add_subscription(10, self(), <<"my-stream">>, State0),
    ?assertMatch(#state{subscriptions = #{10 := _}}, State1),
    ?assertMatch(#state{subscriptions_by_stream = #{<<"my-stream">> := _}}, State1),
    ?assertMatch([10], lake_connection_state:subscription_ids_from_stream(<<"my-stream">>, State1)),
    ?assertMatch([], lake_connection_state:subscription_ids_from_stream(<<"unknown">>, State1)),
    State2 = lake_connection_state:remove_subscription(10, State1),
    ?assertEqual(State0#state{subscriptions = #{}, subscriptions_by_stream = #{}}, State2),
    State3 = lake_connection_state:add_subscription(20, self(), <<"my-stream">>, State1),
    ?assertMatch(
        [10, 20], lake_connection_state:subscription_ids_from_stream(<<"my-stream">>, State3)
    ),
    State4 = lake_connection_state:remove_subscription(10, State3),
    ?assertMatch([20], lake_connection_state:subscription_ids_from_stream(<<"my-stream">>, State4)).

publisher_test() ->
    State0 = lake_connection_state:new(ignored, ignored, 0),
    State1 = lake_connection_state:add_publisher(10, self(), <<"my-stream">>, State0),
    ?assertMatch(#state{publishers = #{10 := _}}, State1),
    ?assertMatch(#state{publishers_by_stream = #{<<"my-stream">> := _}}, State1),
    ?assertMatch([10], lake_connection_state:publisher_ids_from_stream(<<"my-stream">>, State1)),
    ?assertMatch([], lake_connection_state:publisher_ids_from_stream(<<"unknown">>, State1)),
    State2 = lake_connection_state:remove_publisher(10, State1),
    ?assertEqual(State0#state{publishers = #{}, publishers_by_stream = #{}}, State2),
    State3 = lake_connection_state:add_publisher(20, self(), <<"my-stream">>, State1),
    ?assertMatch(
        [10, 20], lake_connection_state:publisher_ids_from_stream(<<"my-stream">>, State3)
    ),
    State4 = lake_connection_state:remove_publisher(10, State3),
    ?assertMatch([20], lake_connection_state:publisher_ids_from_stream(<<"my-stream">>, State4)).

inc_correlation_id_test() ->
    State0 = lake_connection_state:new(ignored, 0, 0),
    ?assertMatch(#state{correlation_id = 1}, lake_connection_state:inc_correlation_id(State0)).

stream_subscriptions_test() ->
    State0 = lake_connection_state:new(ignored, ignored, 0),
    State1 = lake_connection_state:add_subscription(10, self(), <<"my-stream">>, State0),
    State2 = lake_connection_state:add_subscription(20, self(), <<"my-stream">>, State1),
    Self = self(),
    ?assertMatch([Self, Self], lake_connection_state:stream_subscriptions(<<"my-stream">>, State2)).

stream_publishers_test() ->
    State0 = lake_connection_state:new(ignored, ignored, 0),
    State1 = lake_connection_state:add_publisher(10, self(), <<"my-stream">>, State0),
    State2 = lake_connection_state:add_publisher(20, self(), <<"my-stream">>, State1),
    Self = self(),
    ?assertMatch([Self, Self], lake_connection_state:stream_publishers(<<"my-stream">>, State2)).

pending_request_test() ->
    State0 = lake_connection_state:new(ignored, ignored, 0),
    State1 = lake_connection_state:register_pending_request(
        1, requesting_process, extra_info, State0
    ),
    ?assertMatch(
        requesting_process, lake_connection_state:requestor_from_correlation_id(1, State1)
    ),
    State2 = lake_connection_state:remove_pending_request(1, State1),
    ?assertEqual(#{}, State2#state.pending_requests).
