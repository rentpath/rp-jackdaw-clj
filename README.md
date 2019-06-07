# rp-jackdaw-clj

A Clojure library providing various components (using [Stuart Sierra's Component library](https://github.com/stuartsierra/component)) for interacting with Kafka using the [Jackdaw library](https://github.com/FundingCircle/jackdaw).

The components include:
- a stream processor
- a "subscriber" (a simple example of a stream processor that can be used like a consumer, but built on top of the Streams API instead of the lower-level Consumer API)
- a producer
- a topic registry (a dependency of the other components; a topic registry wraps a map of topic metadata and optional serde resolver config (schema registry url and type registry))
- mock processor and registry components for use in tests

Some misc utilities are also included:
- a `user` namespace includes some functions intended for use in the REPL for:
  - listing topics
  - creating topics
  - publishing to a topic
  - consuming from a topic
- a `schema` namespace include a function to fetch a specific version of an Avro schema from a Confluent Schema Registry
- a `state-store` namespace includes some functions for dealing with state stores (only useful if you're using the lower-level Processor API)
- a `streams-extras` namespace includes a few odds and ends that I couldn't find in `jackdaw` that may be useful inside a topology builder

## Usage

Refer to the `comment` blocks at the end of some of the source files for some REPL-friendly examples:
- [processor](src/rp/jackdaw/processor.clj)
- [producer](src/rp/jackdaw/producer.clj)

Refer to the [integration test](test/rp/jackdaw/integration_test.clj) for an example of how to test a topology builder using the mock components.

### Testing recommendations

For a processor, we recommend testing the topology via mocks as shown in the integration test. The mock components used in that test don't require access to a running Kafka cluster.

For a subscriber, we recommend unit testing the callback function itself (which has no dependence on Kafka).

You may also want to create an end-to-end system test that uses non-mock components and connects to an actual Kafka cluster.
It's hard to make a specific recommendation for that since that are many ways you might run your test cluster.

## License

Copyright © 2019 RentPath, LLC

Distributed under the MIT License.
