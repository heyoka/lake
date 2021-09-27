-module(lake_messages).

-export([parse/1]).

-export([
    peer_properties/2,
    sasl_handshake/1,
    sasl_authenticate/4,
    open/2,
    tune/2,
    declare_publisher/4,
    publish/2,
    query_publisher_sequence/3,
    delete_publisher/2,
    credit/2,
    create/3,
    delete/2,
    subscribe/6,
    store_offset/3,
    query_offset/3,
    unsubscribe/2,
    metadata/2
]).

-export([chunk_to_messages/1]).

-include("response_codes.hrl").

-define(REQUEST, 0).
-define(RESPONSE, 1).
-define(VERSION, 1).

-define(DECLARE_PUBLISHER, 1).
-define(PUBLISH, 2).
-define(PUBLISH_CONFIRM, 3).
-define(PUBLISH_ERROR, 4).
-define(QUERY_PUBLISHER_SEQUENCE, 5).
-define(DELETE_PUBLISHER, 6).
-define(SUBSCRIBE, 7).
-define(DELIVER, 8).
-define(CREDIT, 9).
-define(STORE_OFFSET, 10).
-define(QUERY_OFFSET, 11).
-define(UNSUBSCRIBE, 12).
-define(CREATE, 13).
-define(DELETE, 14).
-define(METADATA, 15).
-define(PEER_PROPERTIES, 17).
-define(SASL_HANDSHAKE, 18).
-define(SASL_AUTHENTICATE, 19).
-define(TUNE, 20).
-define(OPEN, 21).

parse(
    <<?RESPONSE:1, ?DECLARE_PUBLISHER:15, ?VERSION:16, Corr:32, ResponseCode:16>>
) ->
    {declare_publisher_response, Corr, ResponseCode};
parse(
    <<?PUBLISH_CONFIRM:16, ?VERSION:16, PublisherId:8, PublishingIdCount:32, PublishingIds/binary>>
) ->
    {publish_confirm, PublisherId, PublishingIdCount, parse_list_of_longs(PublishingIds, [])};
parse(<<?PUBLISH_ERROR:16, ?VERSION:16, PublisherId:8, PublishingIdCount:32, Details/binary>>) ->
    ErrorById = [{Id, Code} || <<Id:64, Code:16>> <= Details],
    {publish_error, PublisherId, PublishingIdCount, ErrorById};
parse(
    <<?RESPONSE:1, ?QUERY_PUBLISHER_SEQUENCE:15, ?VERSION:16, Corr:32, ResponseCode:16,
        Sequence:64>>
) ->
    {query_publisher_sequence_response, Corr, ResponseCode, Sequence};
parse(<<?RESPONSE:1, ?DELETE_PUBLISHER:15, ?VERSION:16, Corr:32, ResponseCode:16>>) ->
    {delete_publisher_response, Corr, ResponseCode};
parse(<<?RESPONSE:1, ?CREDIT:15, ?VERSION:16, ResponseCode:16, SubscriptionId:8>>) ->
    {credit_response, SubscriptionId, ResponseCode};
parse(
    <<?RESPONSE:1, ?SUBSCRIBE:15, ?VERSION:16, Corr:32, ResponseCode:16>>
) ->
    {subscribe_response, Corr, ResponseCode};
parse(
    <<?DELIVER:16, ?VERSION:16, SubscriptionId:8, OsirisChunk/binary>>
) ->
    {deliver, SubscriptionId, OsirisChunk};
parse(<<?RESPONSE:1, ?QUERY_OFFSET:15, ?VERSION:16, Corr:32, ResponseCode:16, Offset:64>>) ->
    {query_offset_response, Corr, ResponseCode, Offset};
parse(
    <<?RESPONSE:1, ?UNSUBSCRIBE:15, ?VERSION:16, Corr:32, ResponseCode:16>>
) ->
    {unsubscribe_response, Corr, ResponseCode};
parse(
    <<?RESPONSE:1, ?CREATE:15, ?VERSION:16, Corr:32, ResponseCode:16>>
) ->
    {create_response, Corr, ResponseCode};
parse(
    <<?RESPONSE:1, ?DELETE:15, ?VERSION:16, Corr:32, ResponseCode:16>>
) ->
    {delete_response, Corr, ResponseCode};
parse(
  <<?RESPONSE:1, ?METADATA:15, ?VERSION:16, Corr:32, Metadata0/binary>>
 ) ->
    <<NumEndpoints:32,EndpointsAndMetadata/binary>> = Metadata0,
    {Endpoints, Rest} =
        parse_endpoints(NumEndpoints, EndpointsAndMetadata),
    StreamsMetadata =
        parse_streams_metadata(Rest),
    {metadata, Corr, Endpoints, StreamsMetadata};
parse(
    <<?RESPONSE:1, ?PEER_PROPERTIES:15, ?VERSION:16, Corr:32, ResponseCode:16,
        _PeerPropertiesCount:32, PeerProperties/binary>>
) ->
    {ok, {peer_properties_response, Corr, ResponseCode, parse_map(PeerProperties)}};
parse(
    <<?RESPONSE:1, ?SASL_HANDSHAKE:15, ?VERSION:16, Corr:32, ResponseCode:16, _MechanismsCount:32,
        Mechanisms/binary>>
) ->
    {ok, {sasl_handshake_response, Corr, ResponseCode, parse_list_of_strings(Mechanisms)}};
parse(
    <<?RESPONSE:1, ?SASL_AUTHENTICATE:15, ?VERSION:16, Corr:32, ResponseCode:16, SaslOpaque/binary>>
) ->
    {ok, {sasl_authenticate_response, Corr, ResponseCode, SaslOpaque}};
parse(
    <<?REQUEST:1, ?TUNE:15, ?VERSION:16, FrameMax:32, Heartbeat:32>>
) ->
    {ok, {tune, FrameMax, Heartbeat}};
parse(
    <<?RESPONSE:1, ?OPEN:15, ?VERSION:16, Corr:32, ?RESPONSE_OK:16, _ConnectionPropertiesCount:32,
        ConnectionProperties/binary>>
) ->
    {ok, {open_response, Corr, ?RESPONSE_OK, parse_map(ConnectionProperties)}};
parse(
    <<?RESPONSE:1, ?OPEN:15, ?VERSION:16, Corr:32, ResponseCode:16>>
) ->
    {ok, {open_response, Corr, ResponseCode}};
parse(Unknown) ->
    {error, {unknown, Unknown}}.

-define(OSIRIS_MAGIC, 5).
-define(OSIRIS_VERSION, 0).
-define(OSIRIS_CHUNK_TYPE_USER, 0).
chunk_to_messages(
    <<?OSIRIS_MAGIC:4, ?OSIRIS_VERSION:4, ?OSIRIS_CHUNK_TYPE_USER:8, NumberOfEntries:16,
        NumberOfRecords:32, Timestamp:64, Epoch:64, ChunkId:64, DataCRC:32, DataLength:32,
        _TrailerLength:32, _Reserved:32, Data:DataLength/binary, _Trailer/binary>>
) ->
    case erlang:crc32(Data) of
        DataCRC ->
            %% FIXME why isn't Trailer of length TrailerLength?
            Messages = parse_data(Data, []),
            Info = #{
                chunk_id => ChunkId,
                number_of_entries => NumberOfEntries,
                number_of_records => NumberOfRecords,
                timestamp => Timestamp,
                epoch => Epoch
            },
            {ok, {Messages, Info}};
        MismatchingCRC ->
            {error, {crc_mismatch, [{expected, DataCRC}, {received, MismatchingCRC}]}}
    end;
chunk_to_messages(Other) ->
    {error, {invalid_osiris_chunk, Other}}.

parse_data(<<>>, Acc) ->
    lists:reverse(Acc);
parse_data(<<0:1, Size:31, Data:Size/binary, Rest/binary>>, Acc) ->
    parse_data(Rest, [Data | Acc]).

peer_properties(CorrelationId, Properties) ->
    PropertiesCount = length(Properties),
    EncodedProperties = encode_keywords(Properties),
    <<
        ?REQUEST:1,
        ?PEER_PROPERTIES:15,
        ?VERSION:16,
        CorrelationId:32,
        PropertiesCount:32,
        EncodedProperties/binary
    >>.

sasl_handshake(CorrelationId) ->
    <<
        ?REQUEST:1,
        ?SASL_HANDSHAKE:15,
        ?VERSION:16,
        CorrelationId:32
    >>.

tune(FrameMax, Heartbeat) ->
    <<
        ?REQUEST:1,
        ?TUNE:15,
        ?VERSION:16,
        FrameMax:32,
        Heartbeat:32
    >>.

sasl_authenticate(CorrelationId, Mechanism = <<"PLAIN">>, User, Password) ->
    MechanismSize = byte_size(Mechanism),
    Fragment = <<0:8, User/binary, 0:8, Password/binary>>,
    FragmentSize = byte_size(Fragment),
    <<
        ?REQUEST:1,
        ?SASL_AUTHENTICATE:15,
        ?VERSION:16,
        CorrelationId:32,
        MechanismSize:16,
        Mechanism:MechanismSize/binary,
        FragmentSize:32,
        Fragment:FragmentSize/binary
    >>.

open(CorrelationId, Vhost) ->
    VhostSize = byte_size(Vhost),
    <<
        ?REQUEST:1,
        ?OPEN:15,
        ?VERSION:16,
        CorrelationId:32,
        VhostSize:16,
        Vhost:VhostSize/binary
    >>.

declare_publisher(CorrelationId, Stream, PublisherId, PublisherReference) ->
    StreamSize = byte_size(Stream),
    PublisherReferenceSize = byte_size(PublisherReference),
    <<
        ?REQUEST:1,
        ?DECLARE_PUBLISHER:15,
        ?VERSION:16,
        CorrelationId:32,
        PublisherId:8,
        PublisherReferenceSize:16,
        PublisherReference:PublisherReferenceSize/binary,
        StreamSize:16,
        Stream:StreamSize/binary
    >>.

publish(PublisherId, Messages) ->
    MessageCount = length(Messages),
    EncodedMessages = encode_messages(Messages),
    <<
        ?PUBLISH:16,
        ?VERSION:16,
        PublisherId:8,
        MessageCount:32,
        EncodedMessages/binary
    >>.

query_publisher_sequence(CorrelationId, PublisherReference, Stream) ->
    PublisherReferenceSize = byte_size(PublisherReference),
    StreamSize = byte_size(Stream),
    <<
        ?REQUEST:1,
        ?QUERY_PUBLISHER_SEQUENCE:15,
        ?VERSION:16,
        CorrelationId:32,
        PublisherReferenceSize:16,
        PublisherReference:PublisherReferenceSize/binary,
        StreamSize:16,
        Stream:StreamSize/binary
    >>.

delete_publisher(CorrelationId, PublisherId) ->
    <<
        ?REQUEST:1,
        ?DELETE_PUBLISHER:15,
        ?VERSION:16,
        CorrelationId:32,
        PublisherId:8
    >>.

credit(SubscriptionId, Credit) ->
    <<
        ?REQUEST:1,
        ?CREDIT:15,
        ?VERSION:16,
        SubscriptionId:8,
        Credit:16
    >>.

encode_messages(Messages) ->
    encode_messages(Messages, <<>>).

encode_messages([], Acc) ->
    Acc;
encode_messages([{Id, Message} | Rest], Acc) when is_integer(Id), is_binary(Message) ->
    Size = byte_size(Message),
    encode_messages(Rest, <<Acc/binary, Id:64, 0:1, Size:31, Message:Size/binary>>).

subscribe(CorrelationId, Stream, SubscriptionId, OffsetDefinition, Credit, Properties) ->
    StreamSize = byte_size(Stream),
    OffsetBin =
        case OffsetDefinition of
            first -> <<1:16>>;
            last -> <<2:16>>;
            next -> <<3:16>>;
            {offset, Offset} -> <<4:16, Offset:64>>;
            {timestamp, Offset} -> <<5:16, Offset:64/signed>>
        end,
    EncodedProperties = encode_keywords(Properties),
    PropertiesCount = length(Properties),
    <<
        ?REQUEST:1,
        ?SUBSCRIBE:15,
        ?VERSION:16,
        CorrelationId:32,
        SubscriptionId:8,
        StreamSize:16,
        Stream:StreamSize/binary,
        OffsetBin/binary,
        Credit:16,
        PropertiesCount:32,
        EncodedProperties/binary
    >>.

store_offset(PublisherReference, Stream, Offset) ->
    PublisherReferenceSize = byte_size(PublisherReference),
    StreamSize = byte_size(Stream),
    <<
      ?STORE_OFFSET:16,
      ?VERSION:16,
      PublisherReferenceSize:16,
      PublisherReference:PublisherReferenceSize/binary,
      StreamSize:16,
      Stream:StreamSize/binary,
      Offset:64
    >>.

query_offset(CorrelationId, PublisherReference, Stream) ->
    PublisherReferenceSize = byte_size(PublisherReference),
    StreamSize = byte_size(Stream),

    <<
      ?REQUEST:1,
      ?QUERY_OFFSET:15,
      ?VERSION:16,
      CorrelationId:32,
      PublisherReferenceSize:16,
      PublisherReference:PublisherReferenceSize/binary,
      StreamSize:16,
      Stream:StreamSize/binary
    >>.

unsubscribe(CorrelationId, SubscriptionId) ->
    <<
        ?REQUEST:1,
        ?UNSUBSCRIBE:15,
        ?VERSION:16,
        CorrelationId:32,
        SubscriptionId:8
    >>.

create(CorrelationId, Stream, Arguments0) ->
    StreamSize = byte_size(Stream),
    ArgumentsCount = length(Arguments0),
    Arguments = encode_keywords(Arguments0),
    <<
        ?REQUEST:1,
        ?CREATE:15,
        ?VERSION:16,
        CorrelationId:32,
        StreamSize:16,
        Stream:StreamSize/binary,
        ArgumentsCount:32,
        Arguments/binary
    >>.

delete(CorrelationId, Stream) ->
    StreamSize = byte_size(Stream),
    <<
        ?REQUEST:1,
        ?DELETE:15,
        ?VERSION:16,
        CorrelationId:32,
        StreamSize:16,
        Stream:StreamSize/binary
    >>.

metadata(CorrelationId, Streams) ->
    StreamsCount = length(Streams),
    EncodedStreams = encode_list_of_strings(Streams),
    <<
      ?REQUEST:1,
      ?METADATA:15,
      ?VERSION:16,
      CorrelationId:32,
      StreamsCount:32,
      EncodedStreams/binary
    >>.

parse_map(Bin) when is_binary(Bin) ->
    parse_map(Bin, #{}).

parse_map(<<>>, Acc) ->
    Acc;
parse_map(
    <<KeySize:16, Key:KeySize/binary, ValueSize:16, Value:ValueSize/binary, Rest/binary>>, Acc
) ->
    parse_map(Rest, Acc#{Key => Value}).

parse_list_of_strings(Bin) ->
    parse_list_of_strings(Bin, []).

parse_list_of_strings(<<>>, Acc) ->
    lists:reverse(Acc);
parse_list_of_strings(<<Size:16, Mechanism:Size/binary, Rest/binary>>, Acc) ->
    parse_list_of_strings(Rest, [Mechanism | Acc]).

parse_list_of_longs(<<>>, Acc) ->
    lists:reverse(Acc);
parse_list_of_longs(<<Id:64, Rest/binary>>, Acc) ->
    parse_list_of_longs(Rest, [Id | Acc]).

encode_keywords(Keywords) ->
    encode_keywords(Keywords, <<>>).

encode_keywords([], Acc) ->
    Acc;
encode_keywords([{Key, Value} | Rest], Acc) ->
    SizeKey = byte_size(Key),
    SizeValue = byte_size(Value),
    encode_keywords(Rest, <<Acc/binary, SizeKey:16, Key/binary, SizeValue:16, Value/binary>>).

encode_list_of_strings(List) ->
    encode_list_of_strings(List, <<>>).

encode_list_of_strings([], Acc) ->
    Acc;
encode_list_of_strings([Bin | Rest], Acc) ->
    Size = byte_size(Bin),
    encode_list_of_strings(Rest, <<Acc/binary, Size:16, Bin:Size/binary>>).

parse_endpoints(NumEndpoints, Bin) ->
    parse_endpoints(NumEndpoints, Bin, []).

parse_endpoints(0, Rest, Endpoints) ->
    {Endpoints, Rest};
parse_endpoints(Cnt, <<Index:16,HostLength:16,Host:HostLength/binary,Port:32,Rest/binary>>, Acc) ->
    EndpointMetadata = #{
                         index => Index,
                         host => Host,
                         port => Port
                        },
    parse_endpoints(Cnt-1, Rest, [EndpointMetadata | Acc]).

parse_streams_metadata(<<NumStreams:32,Meta/binary>>) ->
    parse_streams_metadata(NumStreams, Meta, []).

parse_streams_metadata(0, <<>>, Acc) ->
    Acc;
%% Error case
parse_streams_metadata(NumStreams, <<StreamLength:16,Stream:StreamLength/binary,Code:16,-1:16/signed,0:32, Rest/binary>>, Acc) ->
    StreamMetadata = #{
                       stream => Stream,
                       code => Code
                      },
    parse_streams_metadata(NumStreams - 1, Rest, [StreamMetadata|Acc]);
%% Success case
parse_streams_metadata(NumStreams, <<StreamLength:16,Stream:StreamLength/binary,?RESPONSE_OK:16,LeaderIndex:16,ReplicasCount:32,ReplicasAndRest/binary>>, Acc) ->
    ReplicasSize = ReplicasCount * 2,
    <<ReplicasBin:ReplicasSize/binary,Rest/binary>> = ReplicasAndRest,
    Replicas = [ EndpointIndex || <<EndpointIndex:16>> <= ReplicasBin],
    StreamMetadata = #{
                       stream => Stream,
                       leader_index => LeaderIndex,
                       replicas_count => ReplicasCount,
                       replicas => Replicas
                      },
    parse_streams_metadata(NumStreams - 1, Rest, [StreamMetadata|Acc]).
