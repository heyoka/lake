-module(lake_connection).

-export([connect/6, stop/1]).
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

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2
]).

-include("response_codes.hrl").

connect(Host, Port, User, Password, Vhost, Options) ->
    case lake_raw_connection:connect(Host, Port, User, Password, Vhost, Options) of
        {ok, {Socket, _FrameMax, NegotiatedHeartbeat}} ->
            %% FIXME configure connection with FrameMax
            {ok, Connection} = gen_server:start_link(
                ?MODULE, [_CorrelationId = 3, NegotiatedHeartbeat], []
            ),
            ok = gen_tcp:controlling_process(Socket, Connection),
            Connection ! {{socket, Socket}, self()},
            receive
                received_socket -> ok
            after 5000 ->
                exit(timeout)
            end,
            {ok, Connection};
        E = {error, _} ->
            E
    end.

%% FIXME creating the message binaries should be done in the caller if possible
%% (not possible if the caller needs to be blocked, ie. for requests with
%% CorrId)
declare_publisher(Connection, Stream, PublisherId, PublisherReference) ->
    Result = gen_server:call(
        Connection, {declare_publisher, self(), Stream, PublisherId, PublisherReference}
    ),
    case Result of
        {declare_publisher_response, _, ?RESPONSE_OK} ->
            ok;
        {declare_publisher_response, _, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

publish_sync(Connection, PublisherId, Messages) ->
    %% FIXME encode messages here?
    %% We need to wait for all confirmations
    MessageCount = length(Messages),
    ok = gen_server:call(Connection, {publish_async, PublisherId, Messages}),
    wait_for_confirmations(MessageCount, []).

wait_for_confirmations(0, PublishingIds) ->
    [
        case Result of
            {Id, Code} ->
                {Id, lake_utils:response_code_to_atom(Code)};
            Id ->
                {Id, lake_utils:response_code_to_atom(?RESPONSE_OK)}
        end
     || Result <- PublishingIds
    ];
wait_for_confirmations(Count, PublishingIds0) ->
    %% FIXME if the connection dies while we wait here, will this process also stopped?
    receive
        {publish_confirm, _PublisherId, PublishingIdCount, PublishingIds} ->
            wait_for_confirmations(Count - PublishingIdCount, PublishingIds ++ PublishingIds0);
        {publish_error, _PublisherId, PublishingIdCount, PublishingCodeById} ->
            wait_for_confirmations(Count - PublishingIdCount, PublishingCodeById ++ PublishingIds0)
    end.

query_publisher_sequence(Connection, PublisherReference, Stream) ->
    case gen_server:call(Connection, {query_publisher_sequence, PublisherReference, Stream}) of
        {query_publisher_sequence_response, _, ?RESPONSE_OK, Sequence} ->
            {ok, Sequence};
        {query_publisher_sequence_response, _, ResponseCode, _} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

delete_publisher(Connection, PublisherId) ->
    case gen_server:call(Connection, {delete_publisher, PublisherId}) of
        {delete_publisher_response, _, ?RESPONSE_OK} ->
            ok;
        {delete_publisher_response, _, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

credit_async(Connection, SubscriptionId, Credit) ->
    gen_server:call(Connection, {credit, SubscriptionId, Credit}).

create(Connection, Stream, Arguments) ->
    case gen_server:call(Connection, {create, Stream, Arguments}) of
        {create_response, _, ?RESPONSE_OK} ->
            ok;
        {create_response, _, ?RESPONSE_STREAM_ALREADY_EXISTS} ->
            ok;
        {create_response, _, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

delete(Connection, Stream) ->
    case gen_server:call(Connection, {delete, Stream}) of
        {delete_response, _, ?RESPONSE_OK} ->
            ok;
        {delete_response, _, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

subscribe(Connection, Stream, SubscriptionId, OffsetDefinition, Credit, Properties) ->
    Subscriber = self(),
    Result = gen_server:call(
        Connection,
        {subscribe, Subscriber, Stream, SubscriptionId, OffsetDefinition, Credit, Properties}
    ),
    case Result of
        {subscribe_response, _, ?RESPONSE_OK} ->
            ok;
        {subscribe_response, _, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

store_offset(Connection, PublisherReference, Stream, Offset) ->
    gen_server:call(Connection, {store_offset, PublisherReference, Stream, Offset}).

query_offset(Connection, PublisherReference, Stream) ->
    case gen_server:call(Connection, {query_offset, PublisherReference, Stream}) of
        {query_offset_response, _, ?RESPONSE_OK, Offset} ->
            {ok, Offset};
        {query_offset_response, _, ResponseCode, _Offset} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

unsubscribe(Connection, SubscriptionId) ->
    case gen_server:call(Connection, {unsubscribe, SubscriptionId}) of
        {unsubscribe_response, _CorrelationId, ?RESPONSE_OK} ->
            ok;
        {unsubscribe_response, _CorrelationId, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

metadata(Connection, Streams) ->
    {metadata_response, _CorrelationId, Endpoints, Metadata} = gen_server:call(
        Connection, {metadata, Streams}
    ),
    {ok, Endpoints, Metadata}.

stop(Connection) ->
    case gen_server:call(Connection, {close, ?RESPONSE_OK, <<"stop">>}) of
        {close_response, _Corr, ?RESPONSE_OK} ->
            ok;
        {close_response, _Corr, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

%%
%% Connection
%%
-record(state, {
    correlation_id,
    socket,
    %% packets might be split over multiple tcp packets. This buffer holds incomplete packets.
    rx_buf = <<>>,
    %% FIXME clean-up if caller dies
    pending_requests = #{},
    subscriptions = #{},
    subscriptions_by_stream = #{},
    publishers = #{},
    publishers_by_stream = #{},
    heartbeat
}).

init([InitialCorrelationId, Heartbeat]) ->
    {ok, wait_for_socket, {continue, {InitialCorrelationId, Heartbeat}}}.

handle_continue({CorrelationId, Heartbeat}, wait_for_socket) ->
    receive
        {{socket, Socket}, Sender} ->
            Sender ! received_socket,
            {ok, HeartbeatProcess} = lake_heartbeat:start_link(Heartbeat, self()),
            {noreply, #state{
                correlation_id = CorrelationId, socket = Socket, heartbeat = HeartbeatProcess
            }}
    after 5000 ->
        exit(timeout)
    end.

handle_call({declare_publisher, Publisher, Stream, PublisherId, PublisherReference}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    DeclarePublisher = lake_messages:declare_publisher(
        Corr, Stream, PublisherId, PublisherReference
    ),
    ok = lake_utils:send_message(Socket, DeclarePublisher),
    State1 = register_pending_request(State0, Corr, From, {Publisher, PublisherId, Stream}),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({publish_async, PublisherId, Messages}, From, State) ->
    Socket = State#state.socket,
    Publish = lake_messages:publish(PublisherId, Messages),
    gen_server:reply(From, ok),
    ok = lake_utils:send_message(Socket, Publish),
    {noreply, State};
handle_call({query_publisher_sequence, PublisherReference, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    QueryPublisherSequence = lake_messages:query_publisher_sequence(
        Corr, PublisherReference, Stream
    ),
    ok = lake_utils:send_message(Socket, QueryPublisherSequence),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({delete_publisher, PublisherId}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    DeletePublisher = lake_messages:delete_publisher(Corr, PublisherId),
    ok = lake_utils:send_message(Socket, DeletePublisher),
    State1 = register_pending_request(State0, Corr, From, PublisherId),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({credit, SubscriptionId, Credit}, _From, State) ->
    Socket = State#state.socket,
    case State#state.subscriptions of
        #{SubscriptionId := _} ->
            CreditMessage = lake_messages:credit(SubscriptionId, Credit),
            {reply, lake_utils:send_message(Socket, CreditMessage), State};
        _ ->
            {reply, {error, unknown_subscription}, State}
    end;
handle_call({create, Stream, Arguments}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Create = lake_messages:create(Corr, Stream, Arguments),
    ok = lake_utils:send_message(Socket, Create),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({delete, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Delete = lake_messages:delete(Corr, Stream),
    ok = lake_utils:send_message(Socket, Delete),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call(
    {subscribe, Subscriber, Stream, SubscriptionId, OffsetDefinition, Credit, Properties},
    From,
    State0
) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Subscribe = lake_messages:subscribe(
        Corr, Stream, SubscriptionId, OffsetDefinition, Credit, Properties
    ),
    ok = lake_utils:send_message(Socket, Subscribe),
    State1 = register_pending_request(State0, Corr, From, {Subscriber, SubscriptionId, Stream}),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({store_offset, PublisherReference, Stream, Offset}, _From, State) ->
    Socket = State#state.socket,
    StoreOffset = lake_messages:store_offset(PublisherReference, Stream, Offset),
    {reply, lake_utils:send_message(Socket, StoreOffset), State};
handle_call({query_offset, PublisherReference, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    QueryOffset = lake_messages:query_offset(Corr, PublisherReference, Stream),
    ok = lake_utils:send_message(Socket, QueryOffset),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({unsubscribe, SubscriptionId}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Unsubscribe = lake_messages:unsubscribe(Corr, SubscriptionId),
    ok = lake_utils:send_message(Socket, Unsubscribe),
    State1 = register_pending_request(State0, Corr, From, SubscriptionId),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({metadata, Streams}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Metadata = lake_messages:metadata(Corr, Streams),
    ok = lake_utils:send_message(Socket, Metadata),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({close, ResponseCode, Reason}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Close = lake_messages:close(Corr, ResponseCode, Reason),
    ok = lake_utils:send_message(Socket, Close),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({debug, forward, Message}, _From, State) ->
    %% This call is meant for testing.
    {reply, gen_tcp:send(State#state.socket, Message), State};
handle_call(Call, _From, State) ->
    {reply, {unknown, Call}, State}.

handle_cast(heartbeat, State) ->
    Socket = State#state.socket,
    Heartbeat = lake_messages:heartbeat(),
    ok = lake_utils:send_message(Socket, Heartbeat),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

%% FIXME can we avoid parsing here? Only determine where the message should end up and forward_and_reply it to the owner
%% This might require to copy the binary slices (if the buffer holds multiple messages) - a large message might be referenced by multiple subscribers.
handle_info({tcp, Socket, Packet}, State0 = #state{socket = Socket, rx_buf = Buf0}) ->
    inet:setopts(Socket, [{active, once}]),
    {Messages, BufRest} = split_into_messages(<<Buf0/binary, Packet/binary>>),
    State1 = add_new_subscriptions(Messages, State0),
    State2 = add_new_publishers(Messages, State1),
    forward_and_reply(Messages, State2),
    State3 = clean_publishers(Messages, State2),
    State4 = clean_subscriptions(Messages, State3),
    State5 = clean_pending_requests(Messages, State4),
    State6 = State5#state{rx_buf = BufRest},
    %% Check if the remote told us to close the connection
    case lists:keyfind(close, 1, Messages) of
        {close, Corr, _ResponseCode, Reason} ->
            CloseResponse = lake_messages:close_response(Corr, ?RESPONSE_OK),
            ok = lake_utils:send_message(Socket, CloseResponse),
            {stop, {close, Reason}, State6};
        false ->
            {noreply, State6}
    end;
handle_info({tcp_closed, Port}, State = #state{socket = Port}) ->
    {stop, normal, State}.

terminate(Reason, _State) ->
    Reason.

split_into_messages(Buffer) ->
    split_into_messages(Buffer, []).

split_into_messages(<<Size:32, Buf:Size/binary, Rest/binary>>, Acc) ->
    split_into_messages(Rest, [lake_messages:parse(Buf) | Acc]);
split_into_messages(Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

add_new_subscriptions(Messages, State) ->
    lists:foldl(fun add_new_subscriptions1/2, State, Messages).

add_new_subscriptions1({subscribe_response, Corr, ?RESPONSE_OK}, State0) ->
    #state{pending_requests = #{Corr := {_From, {Subscriber, SubscriptionId, Stream}}}} = State0,
    add_subscription(State0, SubscriptionId, Subscriber, Stream);
add_new_subscriptions1({subscribe_response, _Corr, _ResponseCode}, State) ->
    State;
add_new_subscriptions1(_Other, State) ->
    State.

add_subscription(State, SubscriptionId, Subscriber, Stream) ->
    Subscriptions = State#state.subscriptions,
    State#state{
        subscriptions = Subscriptions#{SubscriptionId => {Subscriber, Stream}},
        subscriptions_by_stream =
            add_to_subscriptions_by_stream(
                State#state.subscriptions_by_stream, Stream, SubscriptionId
            )
    }.

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
            SubscriptionsByStream, Stream, SubscriptionId
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

add_to_subscriptions_by_stream(SubscriptionsByStream, Stream, SubscriptionId) ->
    SubscriptionIds = maps:get(Stream, SubscriptionsByStream, sets:new([{version, 2}])),
    SubscriptionsByStream#{Stream => sets:add_element(SubscriptionId, SubscriptionIds)}.

add_new_publishers(Messages, State) ->
    lists:foldl(fun add_new_publishers1/2, State, Messages).

add_new_publishers1({declare_publisher_response, Corr, ?RESPONSE_OK}, State0) ->
    #state{pending_requests = #{Corr := {_From, {Publisher, PublisherId, Stream}}}} = State0,
    add_publisher(State0, PublisherId, Publisher, Stream);
add_new_publishers1({declare_publisher_response, _Corr, _}, State) ->
    State;
add_new_publishers1(_Other, State) ->
    State.

add_publisher(State, PublisherId, Publisher, Stream) ->
    Publishers = State#state.publishers,
    State#state{
        publishers = Publishers#{PublisherId => Publisher},
        publishers_by_stream = add_to_publishers_by_stream(
            State#state.publishers_by_stream, Stream, PublisherId
        )
    }.

add_to_publishers_by_stream(PublishersByStream, Stream, PublisherId) ->
    Set = maps:get(Stream, PublishersByStream, sets:new([{version, 2}])),
    PublishersByStream#{Stream => sets:add_element(PublisherId, Set)}.

remove_publisher(PublisherId, State) ->
    State#state{publishers = maps:remove(PublisherId, State#state.publishers)}.

forward_and_reply(Messages, State) ->
    lists:foreach(
        fun(Message) -> forward_and_reply1(recipient_from_message(Message, State), Message) end,
        Messages
    ).

forward_and_reply1({send, Pid}, Message) ->
    Pid ! Message;
forward_and_reply1({reply, From}, Message) ->
    gen_server:reply(From, Message);
forward_and_reply1({cast, Pid}, Message) ->
    gen_server:cast(Pid, Message);
forward_and_reply1(List, Message) when is_list(List) ->
    lists:foreach(fun(Recipient) -> forward_and_reply1(Recipient, Message) end, List).

recipient_from_message({declare_publisher_response, Corr, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({publish_confirm, PublisherId, _, _}, State) ->
    recipient_from_publishers(PublisherId, State);
recipient_from_message({publish_error, PublisherId, _, _}, State) ->
    recipient_from_publishers(PublisherId, State);
recipient_from_message({query_publisher_sequence_response, Corr, _, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({delete_publisher_response, Corr, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({subscribe_response, Corr, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({deliver, SubscriptionId, _}, State) ->
    recipient_from_subscriptions(SubscriptionId, State);
recipient_from_message({credit_response, SubscriptionId, _}, State) ->
    recipient_from_subscriptions(SubscriptionId, State);
recipient_from_message({query_offset_response, Corr, _, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({unsubscribe_response, Corr, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({create_response, Corr, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({delete_response, Corr, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({metadata_response, Corr, _, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({metadata_update, _, Stream}, State) ->
    recipients_from_stream(Stream, State);
recipient_from_message({close_response, Corr, _}, State) ->
    recipient_from_corr(Corr, State);
recipient_from_message({close, _, _, _}, _) ->
    %% RabbitMQ sent us a message to close the connection; we need to handle this separately.
    [];
recipient_from_message({heartbeat}, State) ->
    {cast, State#state.heartbeat}.

recipient_from_corr(Corr, State) ->
    #state{
        pending_requests = #{Corr := {From, _Extra}}
    } = State,
    {reply, From}.

recipient_from_publishers(PublisherId, State) ->
    #state{
        publishers = #{PublisherId := Publisher}
    } = State,
    {send, Publisher}.

recipient_from_subscriptions(SubscriptionId, State) ->
    #state{
        subscriptions = #{SubscriptionId := {Subscriber, _Stream}}
    } = State,
    {send, Subscriber}.

recipients_from_stream(Stream, State) ->
    #state{
        subscriptions_by_stream = #{Stream := SubscriptionIds},
        publishers_by_stream = #{Stream := PublisherIds}
    } = State,
    Subscribers =
        [
            begin
                {Subscriber, _Stream} = maps:get(SubscriptionId, State#state.subscriptions),
                {send, Subscriber}
            end
         || SubscriptionId <- sets:to_list(SubscriptionIds)
        ],
    Publishers =
        [
            begin
                {Publisher, _Stream} = maps:get(PublisherId, State#state.subscriptions),
                {send, Publisher}
            end
         || PublisherId <- sets:to_list(PublisherIds)
        ],
    Subscribers ++ Publishers.

clean_publishers(Messages, State) ->
    lists:foldl(fun clean_publishers1/2, State, Messages).

clean_publishers1({delete_publisher_response, Corr, ?RESPONSE_OK}, State0) ->
    #state{pending_requests = #{Corr := {_From, PublisherId}}} = State0,
    remove_publisher(PublisherId, State0);
clean_publishers1({delete_publisher_response, _Corr, _}, State) ->
    State;
clean_publishers1({metadata_update, ?RESPONSE_OK, _Stream}, _State) ->
    exit("MetadataUpdate with ?RESPONSE_OK not handled");
clean_publishers1({metadata_update, _, Stream}, State) ->
    PublisherIds = publisher_ids_from_stream(State, Stream),
    lists:foldl(fun remove_publisher/2, State, PublisherIds);
clean_publishers1(_Other, State) ->
    State.

clean_subscriptions(Messages, State) ->
    lists:foldl(fun clean_subscriptions1/2, State, Messages).

clean_subscriptions1({unsubscribe_response, Corr, ?RESPONSE_OK}, State0) ->
    #state{pending_requests = #{Corr := {_From, SubscriptionId}}} = State0,
    remove_subscription(SubscriptionId, State0);
clean_subscriptions1({unsubscribe_response, _Corr, _}, _State) ->
    exit("UnsubscribeResponse without ?RESPONSE_OK not handled");
clean_subscriptions1({metadata_update, ?RESPONSE_OK, _Stream}, _State) ->
    exit("MetadataUpdate with ?RESPONSE_OK not handled");
clean_subscriptions1({metadata_update, _, Stream}, State) ->
    SubscriptionIds = subscription_ids_from_stream(State, Stream),
    lists:foldl(fun remove_subscription/2, State, SubscriptionIds);
clean_subscriptions1(_Other, State) ->
    State.

clean_pending_requests(Messages, State) ->
    lists:foldl(fun clean_pending_requests1/2, State, Messages).

clean_pending_requests1(Message, State0) ->
    case lake_messages:message_to_correlation_id(Message) of
        {ok, Corr} ->
            #state{
                pending_requests = #{Corr := {From, _Extra}}
            } = State0,
            PendingRequests = maps:remove(From, maps:remove(Corr, State0#state.pending_requests)),
            State0#state{
                pending_requests = PendingRequests
            };
        {error, _} ->
            State0
    end.

register_pending_request(State, Corr, From, Extra) ->
    BlockedCallers0 = State#state.pending_requests,
    BlockedCallers = BlockedCallers0#{Corr => {From, Extra}, From => Corr},
    State#state{
        pending_requests = BlockedCallers
    }.

inc_correlation_id(State) ->
    Corr = State#state.correlation_id,
    State#state{correlation_id = Corr + 1}.

subscription_ids_from_stream(State, Stream) ->
    case State#state.subscriptions_by_stream of
        #{Stream := Subscriptions} ->
            sets:to_list(Subscriptions);
        _ ->
            []
    end.

publisher_ids_from_stream(State, Stream) ->
    case State#state.publishers_by_stream of
        #{Stream := Publishers} ->
            sets:to_list(Publishers);
        _ ->
            []
    end.
