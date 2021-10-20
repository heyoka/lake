# lake

imported from https://gitlab.com/evnu/lake

An Erlang client for RabbitMQ's [stream plugin](https://www.rabbitmq.com/stream.html).

A stream feeds into a lake, and a lake feeds into a stream.

## Installation

### Rebar3

```erlang
%% rebar.config
{deps, [lake]}.
```

### Mix

```elixir
defp deps do
  [
      {:lake, "~> 0.1"}
  ]
end
```

## Usage

```erlang
example() ->
    {ok, Connection} = lake:connect(host(), port(), <<"guest">>, <<"guest">>, <<"/">>),
    ok = lake:create(Connection, <<"my-stream">>, []),
    ok = lake:subscribe(Connection, <<"my-stream">>, SubscriptionId = 1, first, 1000, []),
    ok = lake:declare_publisher(Connection, <<"my-stream">>, PublisherId = 1, <<"my-publisher">>),
    [{1, ok}] = lake:publish_sync(Connection, PublisherId = 1, [{_PublishingId = 1, <<"Hello, World!">>}]),
    {ok, {[Message], _}} =
        receive
            {deliver, _ResponseCode, OsirisChunk} ->
                lake:chunk_to_messages(OsirisChunk)
        after 5000 ->
            exit(timeout)
        end,
    io:format("Received: ~p~n", [Message]),
    ok = lake:unsubscribe(Connection, SubscriptionId),
    ok = lake:delete_publisher(Connection, PublisherId),
    ok = lake:delete(Connection, Stream),
    ok = lake:stop(Connection),
    ok.
```

## Build

```
$ rebar3 compile
```

## Running Tests

```
$ RABBITMQ_HOST=172.17.0.2 rebar3 ct
```

## Benchmark

```
$ cd benchmark
$ rebar3 escriptize && _build/default/bin/benchmark streams://172.17.0.2:5552 guest guest "/"
===> Verifying dependencies...
===> App lake is a checkout dependency and cannot be locked.
===> Analyzing applications...
===> Compiling lake
===> Analyzing applications...
===> Compiling benchmark
===> Building escript for benchmark...
Running benchmark with messages of size 350B and one publisher, one subscriber. Hit enter to stop
2: wrote 40 msgs in 0.00306s (13071 msgs/s), read 0 msgs with 0.998s remaining to full second (0.0 msgs/s)
3: wrote 160 msgs in 0.008447s (18941 msgs/s), read 40 msgs with 0.99s remaining to full second (40.0 msgs/s)
4: wrote 640 msgs in 0.009589s (66743 msgs/s), read 160 msgs with 0.985s remaining to full second (160.0 msgs/s)
5: wrote 2560 msgs in 0.049547s (51668 msgs/s), read 640 msgs with 0.983s remaining to full second (639.3606393606394 msgs/s)
6: wrote 10240 msgs in 0.10561s (96960 msgs/s), read 2560 msgs with 0.944s remaining to full second (2560.0 msgs/s)
7: wrote 40960 msgs in 0.381258s (107433 msgs/s), read 10240 msgs with 0.887s remaining to full second (10240.0 msgs/s)
8: wrote 81920 msgs in 0.616224s (132938 msgs/s), read 40960 msgs with 0.611s remaining to full second (40960.0 msgs/s)
9: wrote 122880 msgs in 0.880724s (139521 msgs/s), read 81920 msgs with 0.375s remaining to full second (81920.0 msgs/s)
...
```

## Implemented Messages

* [x] DeclarePublisher
* [x] Publish
* [x] PublishConfirm
* [x] PublishError
* [x] QueryPublisherSequence
* [x] DeletePublisher
* [x] Subscribe
* [x] Deliver
* [x] Credit
* [x] StoreOffset
* [x] QueryOffset
* [x] Unsubscribe
* [x] Create
* [x] Delete
* [x] Metadata
* [x] MetadataUpdate
* [x] PeerProperties
* [x] SaslHandshake
* [x] SaslAuthenticate
* [x] Tune
* [x] Open
* [x] Close
* [x] Heartbeat
* [ ] Route (experimental)
* [ ] Partitions (experimental)

## Random TODOs

* [ ] Encoding should not happen in the Connection - Encoding crashes must only crash the caller

## License

Copyright 2021 sonnen eServices GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
