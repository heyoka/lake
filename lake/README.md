# lake

An Erlang RabbitMQ Streams Connector.

## Build

    $ rebar3 compile

## Running Tests

    $ RABBITMQ_HOST=172.17.0.2 rebar3 ct

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
* [ ] MetadataUpdate
* [x] PeerProperties
* [x] SaslHandshake
* [x] SaslAuthenticate
* [x] Tune
* [x] Open
* [ ] Close
* [ ] Heartbeat
* [ ] Route (experimental)
* [ ] Partitions (experimental)

## Random TODOs

* [ ] Encoding should not happen in the Connection - Encoding crashes must only crash the caller

## License

Copyright 2019 sonnen eServices GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
