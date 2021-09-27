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
        Connection, {declare_publisher, Stream, PublisherId, PublisherReference}
    ),
    case Result of
        {declare_publisher_response, _, ?RESPONSE_OK} ->
            ok;
        {declare_publisher_response, _, ResponseCode} ->
            {error, lake_utils:response_code_to_atom(ResponseCode)}
    end.

publish_sync(Connection, PublisherId, Messages) ->
    %% FIXME encode messages here?
    case gen_server:call(Connection, {publish_sync, PublisherId, Messages}) of
        {publish_confirm, PublisherId, PublishingIdCount, PublishingIds} ->
            {ok, {PublishingIdCount, PublishingIds}};
        {publish_confirm, WrongPublisherId, _, _} ->
            {error, {wrong_publisher_id, WrongPublisherId}};
        {publish_error, _, PublishingIdCount, PublishingCodeById} ->
            {error, {publish_error, PublishingIdCount, PublishingCodeById}}
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
    {metadata, _CorrelationId, Endpoints, Metadata} = gen_server:call(
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
    logger:info("Tune: FrameMax = ~p, Heatbeat = ~p~n", [FrameMax, Heartbeat]),
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
    %% Pending requests and requestors:
    %% * Maps from correlation ids to blocked callers + extra data, and maps from blocked
    %%   callers to correlation ids to allow cleaning-up if the caller dies
    %% * Maps from PublisherId to publisher and vice versa for sync publishing
    %% FIXME clean-up if caller dies
    pending = #{},
    subscriptions = #{}
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

handle_call({declare_publisher, Stream, PublisherId, PublisherReference}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    DeclarePublisher = lake_messages:declare_publisher(
        Corr, Stream, PublisherId, PublisherReference
    ),
    ok = send_message(Socket, DeclarePublisher),
    State1 = register_pending(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({publish_sync, PublisherId, Messages}, From, State0) ->
    Socket = State0#state.socket,
    Publish = lake_messages:publish(PublisherId, Messages),
    ok = send_message(Socket, Publish),
    State1 = register_pending(State0, PublisherId, From, []),
    {noreply, State1};
handle_call({query_publisher_sequence, PublisherReference, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    QueryPublisherSequence = lake_messages:query_publisher_sequence(
        Corr, PublisherReference, Stream
    ),
    ok = send_message(Socket, QueryPublisherSequence),
    State1 = register_pending(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({delete_publisher, PublisherId}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    DeletePublisher = lake_messages:delete_publisher(Corr, PublisherId),
    ok = send_message(Socket, DeletePublisher),
    State1 = register_pending(State0, Corr, From, []),
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
    State1 = register_pending(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({delete, Stream}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Delete = lake_messages:delete(Corr, Stream),
    ok = send_message(Socket, Delete),
    State1 = register_pending(State0, Corr, From, []),
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
    State1 = register_pending(State0, Corr, From, {Subscriber, SubscriptionId}),
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
    State1 = register_pending(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({unsubscribe, SubscriptionId}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Unsubscribe = lake_messages:unsubscribe(Corr, SubscriptionId),
    ok = send_message(Socket, Unsubscribe),
    State1 = register_pending(State0, Corr, From, SubscriptionId),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call({metadata, Streams}, From, State0) ->
    Corr = State0#state.correlation_id,
    Socket = State0#state.socket,
    Metadata = lake_messages:metadata(Corr, Streams),
    ok = send_message(Socket, Metadata),
    State1 = register_pending(State0, Corr, From, []),
    State2 = inc_correlation_id(State1),
    {noreply, State2};
handle_call(Call, _From, State) ->
    {reply, {unknown, Call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

%% FIXME can we avoid parsing here? Only determine where the message should end up and forward it to the owner
%% This might require to copy the binary slices (if the buffer holds multiple messages) - a large message might be referenced by multiple subscribers.
handle_info({tcp, Socket, Packet}, State0 = #state{socket = Socket, rx_buf = Buf0}) ->
    inet:setopts(Socket, [{active, once}]),
    Buf1 = <<Buf0/binary, Packet/binary>>,
    {MessageByReceiver, BufRest} = split_by_receiver(Buf1),
    State1 = update_subscriptions(State0, MessageByReceiver),
    State2 = forward_to_receivers(MessageByReceiver, State1),
    {noreply, State2#state{rx_buf = BufRest}}.

%% FIXME handle_info({tcp_closed, Port}, State)

terminate(Reason, State) ->
    gen_tcp:close(State#state.socket),
    Reason.

%%
%% Helper Functions
%%
split_by_receiver(Buffer) ->
    split_by_receiver(Buffer, []).

split_by_receiver(<<Size:32, Buf:Size/binary, Rest/binary>>, Acc) ->
    Parsed = lake_messages:parse(Buf),
    Receiver = receiver(Parsed),
    split_by_receiver(Rest, [{Receiver, Parsed} | Acc]);
split_by_receiver(Rest, Acc) ->
    {lists:reverse(Acc), Rest}.

receiver({publish_confirm, PublisherId, _, _}) -> {publisher_id, PublisherId};
receiver({deliver, SubscriptionId, _}) -> {subscription_id, SubscriptionId};
receiver({declare_publisher_response, Corr, _}) -> {correlation_id, Corr};
receiver({query_publisher_sequence_response, Corr, _, _}) -> {correlation_id, Corr};
receiver({delete_publisher_response, Corr, _}) -> {correlation_id, Corr};
receiver({create_response, Corr, _}) -> {correlation_id, Corr};
receiver({delete_response, Corr, _}) -> {correlation_id, Corr};
receiver({subscribe_response, Corr, _}) -> {correlation_id, Corr};
receiver({query_offset_response, Corr, _, _}) -> {correlation_id, Corr};
receiver(M = {unsubscribe_response, _, _}) -> M;
receiver({metadata, Corr, _, _}) -> {correlation_id, Corr}.

forward_to_receivers([], State) ->
    State;
forward_to_receivers([{{subscription_id, Id}, Message} | Rest], State) ->
    #state{subscriptions = #{Id := Subscriber}} = State,
    Subscriber ! Message,
    forward_to_receivers(Rest, State);
forward_to_receivers([{{correlation_id, Corr}, Message} | Rest], State) ->
    #{Corr := {From, _Extra}} = State#state.pending,
    gen_server:reply(From, Message),
    forward_to_receivers(Rest, deregister_pending(Corr, State));
forward_to_receivers([{{publisher_id, Id}, Message} | Rest], State) ->
    #{Id := {From, _Extra}} = State#state.pending,
    gen_server:reply(From, Message),
    forward_to_receivers(Rest, deregister_pending(Id, State));
forward_to_receivers([{{unsubscribe_response, Corr, _}, Message} | Rest], State) ->
    #{Corr := {From, _Extra}} = State#state.pending,
    gen_server:reply(From, Message),
    forward_to_receivers(Rest, deregister_pending(Corr, State)).

deregister_pending(Key, State) ->
    #state{
        pending = #{Key := {From, _Extra}}
    } = State,
    State#state{
        pending =
            maps:remove(
                From,
                maps:remove(Key, State#state.pending)
            )
    }.

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

register_pending(State, Corr, From, Extra) ->
    BlockedCallers0 = State#state.pending,
    BlockedCallers = BlockedCallers0#{Corr => {From, Extra}, From => Corr},
    State#state{
        pending = BlockedCallers
    }.

inc_correlation_id(State) ->
    Corr = State#state.correlation_id,
    State#state{correlation_id = Corr + 1}.

update_subscriptions(State, []) ->
    State;
update_subscriptions(State, [{{unsubscribe_response, Corr, ?RESPONSE_OK}, _} | Rest]) ->
    #state{
        pending = #{Corr := {_From, SubscriptionId}},
        subscriptions = Subscriptions
    } = State,
    update_subscriptions(
        State#state{
            subscriptions = maps:remove(SubscriptionId, Subscriptions)
        },
        Rest
    );
update_subscriptions(State, [{{correlation_id, Corr}, Message} | Rest]) ->
    case is_positive_subscribe_response(Message) of
        true ->
            #state{
                pending = #{Corr := {_From, {Subscriber, SubscriptionId}}},
                subscriptions = Subscriptions
            } = State,
            update_subscriptions(
                State#state{
                    subscriptions =
                        Subscriptions#{SubscriptionId => Subscriber}
                },
                Rest
            );
        false ->
            update_subscriptions(State, Rest)
    end;
update_subscriptions(State, [{{subscription_id, _}, _} | Rest]) ->
    update_subscriptions(State, Rest);
update_subscriptions(State, [{{publisher_id, _}, _} | Rest]) ->
    update_subscriptions(State, Rest).

is_positive_subscribe_response({subscribe_response, _, ?RESPONSE_OK}) -> true;
is_positive_subscribe_response(_) -> false.
