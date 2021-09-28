-module(lake_raw_connection).

-export([connect/5]).

-include("response_codes.hrl").

connect(Host, Port, User, Password, Vhost) ->
    try
        connect1(Host, Port, User, Password, Vhost)
    catch
        throw:Reason ->
            {error, Reason}
    end.

connect1(Host, Port, User, Password, Vhost) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {active, once}]),

    peer_properties(Socket),
    sasl_handshake(Socket),
    %% TODO Allow using mechanisms other than <<"PLAIN">> ?
    MaybeTune = sasl_authenticate(Socket, <<"PLAIN">>, User, Password),
    {FrameMax, Heartbeat} = tune(Socket, MaybeTune),
    open(Socket, Vhost),

    {ok, {Socket, FrameMax, Heartbeat}}.

peer_properties(Socket) ->
    PeerProperties = [{<<"platform">>, <<"Erlang">>}],
    lake_utils:send_message(Socket, lake_messages:peer_properties(0, PeerProperties)),
    case wait_for_message(Socket) of
        {{ok, {peer_properties_response, 0, ?RESPONSE_OK, _}}, <<>>} ->
            ok;
        {{ok, {peer_properties_response, 0, ResponseCode, _}}, <<>>} ->
            throw({peer_properties_failed, lake_utils:response_code_to_atom(ResponseCode)})
    end.

sasl_handshake(Socket) ->
    lake_utils:send_message(Socket, lake_messages:sasl_handshake(1)),
    case wait_for_message(Socket) of
        {{ok, {sasl_handshake_response, 1, ?RESPONSE_OK, Mechanisms}}, <<>>} ->
            Mechanisms;
        {{ok, {sasl_handshake_response, 1, ResponseCode, _Mechanisms}}, <<>>} ->
            throw({sasl_handshake_failed, lake_utils:response_code_to_atom(ResponseCode)})
    end.

sasl_authenticate(Socket, Mechanism, User, Password) ->
    lake_utils:send_message(Socket, lake_messages:sasl_authenticate(2, Mechanism, User, Password)),
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
    lake_utils:send_message(Socket, lake_messages:tune(FrameMax, Heartbeat)),
    {FrameMax, Heartbeat}.

open(Socket, Vhost) ->
    lake_utils:send_message(Socket, lake_messages:open(3, Vhost)),
    case wait_for_message(Socket) of
        %% FIXME make use of advertised host and port?
        {{ok, {open_response, 3, ?RESPONSE_OK, _ConnectionProperties}}, <<>>} ->
            ok;
        {{ok, {open_response, 3, ResponseCode}}, <<>>} ->
            throw({open_failed, lake_utils:response_code_to_atom(ResponseCode)})
    end.

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
