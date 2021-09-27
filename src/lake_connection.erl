-module(lake_connection).

-export([connect/5, stop/1]).
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

connect(Host, Port, User, Password, Vhost) ->
    try
        connect1(Host, Port, User, Password, Vhost)
    catch
        throw:Reason ->
            {error, Reason}
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

connect1(Host, Port, User, Password, Vhost) when
    is_binary(User), is_binary(Password), is_binary(Vhost)
->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, once}]),

    peer_properties(Socket),
    sasl_handshake(Socket),
    %% TODO Allow using mechanisms other than <<"PLAIN">> ?
    MaybeTune = sasl_authenticate(Socket, <<"PLAIN">>, User, Password),
    %% FIXME configure connection with FrameMax and Heartbeat
    {_FrameMax, _Heartbeat} = tune(Socket, MaybeTune),
    open(Socket, Vhost),

    {ok, Connection} = gen_server:start_link(?MODULE, [_CorrelationId = 3], []),
    ok = gen_tcp:controlling_process(Socket, Connection),
    Connection ! {{socket, Socket}, self()},
    receive
        received_socket -> ok
    after 5000 ->
        exit(timeout)
    end,

    {ok, Connection}.

peer_properties(Socket) ->
    PeerProperties = [{<<"platform">>, <<"Erlang">>}],
    send_message(Socket, lake_messages:peer_properties(0, PeerProperties)),
    case wait_for_message(Socket) of
        {{ok, {peer_properties_response, 0, ?RESPONSE_OK, _}}, <<>>} ->
            ok;
        {{ok, {peer_properties_response, 0, ResponseCode, _}}, <<>>} ->
            throw({peer_properties_failed, lake_utils:response_code_to_atom(ResponseCode)})
    end.

sasl_handshake(Socket) ->
    send_message(Socket, lake_messages:sasl_handshake(1)),
    case wait_for_message(Socket) of
        {{ok, {sasl_handshake_response, 1, ?RESPONSE_OK, Mechanisms}}, <<>>} ->
            Mechanisms;
        {{ok, {sasl_handshake_response, 1, ResponseCode, _Mechanisms}}, <<>>} ->
            throw({sasl_handshake_failed, lake_utils:response_code_to_atom(ResponseCode)})
    end.

sasl_authenticate(Socket, Mechanism, User, Password) ->
    send_message(Socket, lake_messages:sasl_authenticate(2, Mechanism, User, Password)),
    case wait_for_message(Socket) of
        {{ok, {sasl_authenticate_response, 2, ?RESPONSE_OK, _SaslOpaque}}, Rest} ->
            Rest;
        {{ok, {sasl_authenticate_response, 2, ResponseCode, _SaslOpaque}}, _Rest} ->
            throw({authentication_failed, lake_utils:response_code_to_atom(ResponseCode)})
    end.

tune(Socket, MaybeTune) ->
    {{ok, {tune, FrameMax, Heartbeat}}, <<>>} =
        case MaybeTune of
            <<>> ->
                wait_for_message(Socket);
            <<Size:32, Tune:Size/binary>> ->
                {lake_messages:parse(Tune), <<>>}
        end,
    send_message(Socket, lake_messages:tune(FrameMax, Heartbeat)),
    {FrameMax, Heartbeat}.

open(Socket, Vhost) ->
    send_message(Socket, lake_messages:open(3, Vhost)),
    case wait_for_message(Socket) of
        %% FIXME make use of advertised host and port?
        {{ok, {open_response, 3, ?RESPONSE_OK, _ConnectionProperties}}, <<>>} ->
            ok;
        {{ok, {open_response, 3, ResponseCode}}, <<>>} ->
            throw({open_failed, lake_utils:response_code_to_atom(ResponseCode)})
    end.

stop(Connection) ->
    %% FIXME this should be a graceful CLOSE..
    gen_server:stop(Connection).

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
    publishers_by_stream = #{}
}).

init([CorrelationId]) ->
    %% FIXME Do not use trap_exit - the port will be closed anyways if the process dies
    process_flag(trap_exit, true),
    {ok, wait_for_socket, {continue, CorrelationId}}.

handle_continue(CorrelationId, wait_for_socket) ->
    receive
        {{socket, Socket}, Sender} ->
            Sender ! received_socket,
            {noreply, #state{correlation_id = CorrelationId, socket = Socket}}
    after 5000 ->
        exit(timeout)
    end.

handle_call({declare_publisher, Publisher, Stream, PublisherId, PublisherReference}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    DeclarePublisher = lake_messages:declare_publisher(
        Corr, Stream, PublisherId, PublisherReference
    ),
    ok = send_message(Socket, DeclarePublisher),
    State1 = register_pending_request(State0, Corr, From, {Publisher, PublisherId, Stream}),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({publish_async, PublisherId, Messages}, From, State) ->
    Socket = State#state.socket,
    Publish = lake_messages:publish(PublisherId, Messages),
    gen_server:reply(From, ok),
    ok = send_message(Socket, Publish),
    {noreply, State};
handle_call({query_publisher_sequence, PublisherReference, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    QueryPublisherSequence = lake_messages:query_publisher_sequence(
        Corr, PublisherReference, Stream
    ),
    ok = send_message(Socket, QueryPublisherSequence),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({delete_publisher, PublisherId}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    DeletePublisher = lake_messages:delete_publisher(Corr, PublisherId),
    ok = send_message(Socket, DeletePublisher),
    State1 = register_pending_request(State0, Corr, From, PublisherId),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({credit, SubscriptionId, Credit}, _From, State) ->
    Socket = State#state.socket,
    case State#state.subscriptions of
        #{SubscriptionId := _} ->
            CreditMessage = lake_messages:credit(SubscriptionId, Credit),
            {reply, send_message(Socket, CreditMessage), State};
        _ ->
            {reply, {error, unknown_subscription}, State}
    end;
handle_call({create, Stream, Arguments}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Create = lake_messages:create(Corr, Stream, Arguments),
    ok = send_message(Socket, Create),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({delete, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Delete = lake_messages:delete(Corr, Stream),
    ok = send_message(Socket, Delete),
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
    ok = send_message(Socket, Subscribe),
    State1 = register_pending_request(State0, Corr, From, {Subscriber, SubscriptionId, Stream}),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({store_offset, PublisherReference, Stream, Offset}, _From, State) ->
    Socket = State#state.socket,
    StoreOffset = lake_messages:store_offset(PublisherReference, Stream, Offset),
    {reply, send_message(Socket, StoreOffset), State};
handle_call({query_offset, PublisherReference, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    QueryOffset = lake_messages:query_offset(Corr, PublisherReference, Stream),
    ok = send_message(Socket, QueryOffset),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({unsubscribe, SubscriptionId}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Unsubscribe = lake_messages:unsubscribe(Corr, SubscriptionId),
    ok = send_message(Socket, Unsubscribe),
    State1 = register_pending_request(State0, Corr, From, SubscriptionId),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({metadata, Streams}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Metadata = lake_messages:metadata(Corr, Streams),
    ok = send_message(Socket, Metadata),
    State1 = register_pending_request(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call(Call, _From, State) ->
    {reply, {unknown, Call}, State}.

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
    {noreply, State5#state{rx_buf = BufRest}}.

%% FIXME handle_info({tcp_closed, Port}, State)

terminate(Reason, State) ->
    gen_tcp:close(State#state.socket),
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
    recipients_from_stream(Stream, State).

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

frame(Message) when is_binary(Message) ->
    Size = byte_size(Message),
    <<Size:32, Message/binary>>.

send_message(Socket, Message) ->
    gen_tcp:send(Socket, frame(Message)).

wait_for_message(Socket) ->
    receive
        {tcp, Socket, <<Size:32, Message:Size/binary, Rest/binary>>} ->
            inet:setopts(Socket, [{active, once}]),

            Parsed = lake_messages:parse(Message),
            {Parsed, Rest};
        {tcp, Socket, Other} ->
            {error, {malformed, Other}}
    after 5000 ->
        {error, timeout}
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
