# rp-jackdaw-clj

A Clojure library providing various components (using [Stuart Sierra's Component library](https://github.com/stuartsierra/component)) for interacting with Kafka using the [Jackdaw library](https://github.com/FundingCircle/jackdaw).

The components include:
- a serde resolver
- a topic registry
- a producer
- a stream processor
- a "subscriber" (a simple example of a stream processor that can be used like a consumer, but built on top of the Streams API instead of the lower-level Consumer API)

## Usage

FIXME

## License

Copyright © 2019 RentPath, LLC

Distributed under the MIT License.
