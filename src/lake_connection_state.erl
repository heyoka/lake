-module(lake_connection_state).

-include("lake_connection_state.hrl").

-export([
    new/3,
    publisher_ids_from_stream/2,
    subscription_ids_from_stream/2,
    inc_correlation_id/1,
    register_pending_request/4,
    remove_pending_request/2,
    add_subscription/4,
    remove_subscription/2,
    add_publisher/4,
    remove_publisher/2,
    stream_subscriptions/2,
    stream_publishers/2,
    subscriber_from_subscription_id/2,
    publisher_from_publisher_id/2,
    requestor_from_correlation_id/2
]).

new(Socket, CorrelationId, Heartbeat) ->
    HeartbeatProcess =
        if
            Heartbeat > 0 ->
                {ok, HeartbeatProcess0} = lake_heartbeat:start_link(Heartbeat, self()),
                HeartbeatProcess0;
            true ->
                undefined
        end,
    #state{
        correlation_id = CorrelationId,
        socket = Socket,
        heartbeat = HeartbeatProcess
    }.

publisher_ids_from_stream(Stream, State) ->
    case State#state.publishers_by_stream of
        #{Stream := Publishers} ->
            sets:to_list(Publishers);
        _ ->
            []
    end.

subscription_ids_from_stream(Stream, State) ->
    case State#state.subscriptions_by_stream of
        #{Stream := Subscriptions} ->
            sets:to_list(Subscriptions);
        _ ->
            []
    end.

inc_correlation_id(State = #state{correlation_id = Corr}) ->
    State#state{correlation_id = Corr + 1}.

register_pending_request(Corr, From, Extra, State) ->
    PendingRequests = State#state.pending_requests,
    State#state{
        pending_requests = PendingRequests#{Corr => {From, Extra}, From => Corr}
    }.

remove_pending_request(Corr, State) ->
    #state{
        pending_requests = #{Corr := {From, _Extra}}
    } = State,
    State#state{
        pending_requests = maps:remove(From, maps:remove(Corr, State#state.pending_requests))
    }.

add_subscription(SubscriptionId, Subscriber, Stream, State) ->
    Subscriptions = State#state.subscriptions,
    State#state{
        subscriptions = Subscriptions#{SubscriptionId => {Subscriber, Stream}},
        subscriptions_by_stream =
            add_to_subscriptions_by_stream(
                Stream,
                SubscriptionId,
                State#state.subscriptions_by_stream
            )
    }.

add_to_subscriptions_by_stream(Stream, SubscriptionId, SubscriptionsByStream) ->
    SubscriptionIds = maps:get(Stream, SubscriptionsByStream, sets:new([{version, 2}])),
    SubscriptionsByStream#{Stream => sets:add_element(SubscriptionId, SubscriptionIds)}.

remove_subscription(SubscriptionId, State) ->
    #state{
        subscriptions = Subscriptions,
        subscriptions_by_stream = SubscriptionsByStream
    } = State,
    #{
        SubscriptionId := {_Subscriber, Stream}
    } = Subscriptions,
    State#state{
        subscriptions = maps:remove(SubscriptionId, Subscriptions),
        subscriptions_by_stream = remove_subscription_by_stream(
            SubscriptionsByStream,
            Stream,
            SubscriptionId
        )
    }.

remove_subscription_by_stream(SubscriptionsByStream, Stream, SubscriptionId) ->
    #{Stream := Subscriptions0} = SubscriptionsByStream,
    Subscriptions1 = sets:del_element(SubscriptionId, Subscriptions0),
    case sets:is_empty(Subscriptions1) of
        true ->
            maps:remove(Stream, SubscriptionsByStream);
        false ->
            SubscriptionsByStream#{Stream := Subscriptions1}
    end.

add_publisher(PublisherId, Publisher, Stream, State) ->
    Publishers = State#state.publishers,
    State#state{
        publishers = Publishers#{PublisherId => {Publisher, Stream}},
        publishers_by_stream = add_to_publishers_by_stream(
            State#state.publishers_by_stream,
            Stream,
            PublisherId
        )
    }.

add_to_publishers_by_stream(PublishersByStream, Stream, PublisherId) ->
    Set = maps:get(Stream, PublishersByStream, sets:new([{version, 2}])),
    PublishersByStream#{Stream => sets:add_element(PublisherId, Set)}.

remove_publisher(PublisherId, State) ->
    #state{
        publishers = Publishers,
        publishers_by_stream = PublishersByStream
    } = State,
    #{
        PublisherId := {_Publisher, Stream}
    } = Publishers,
    State#state{
        publishers = maps:remove(PublisherId, State#state.publishers),
        publishers_by_stream = remove_publisher_by_stream(
            PublishersByStream, Stream, PublisherId
        )
    }.

remove_publisher_by_stream(PublishersByStream, Stream, PublisherId) ->
    #{Stream := Publishers0} = PublishersByStream,
    Publishers1 = sets:del_element(PublisherId, Publishers0),
    case sets:is_empty(Publishers1) of
        true ->
            maps:remove(Stream, PublishersByStream);
        false ->
            PublishersByStream#{Stream := Publishers1}
    end.

stream_subscriptions(Stream, State) ->
    #state{subscriptions_by_stream = #{Stream := SubscriptionIds}} = State,
    [
        subscriber_from_subscription_id(SubscriptionId, State)
     || SubscriptionId <- sets:to_list(SubscriptionIds)
    ].

stream_publishers(Stream, State) ->
    #state{publishers_by_stream = #{Stream := PublisherIds}} = State,
    [
        publisher_from_publisher_id(PublisherId, State)
     || PublisherId <- sets:to_list(PublisherIds)
    ].

subscriber_from_subscription_id(SubscriptionId, State) ->
    #state{
        subscriptions = #{SubscriptionId := {Subscriber, _Stream}}
    } = State,
    Subscriber.

publisher_from_publisher_id(PublisherId, State) ->
    #state{
        publishers = #{PublisherId := {Publisher, _Stream}}
    } = State,
    Publisher.

requestor_from_correlation_id(Corr, State) ->
    #state{
        pending_requests = #{Corr := {From, _Extra}}
    } = State,
    From.
