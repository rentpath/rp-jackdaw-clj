(ns rp.jackdaw.state-store
  "Adds a protocol-based wrapper for interacting with KV state stores.
  Includes some store-related utilities as well.
  Nothing really jackdaw-specific here, but including in this library anyway.
  Useful when dealing with explicit state stores using the lower-level Processor API
  (for example via the Streams `transform` method)."
  (:require [cheshire.core :as json])
  (:import [org.apache.kafka.streams.state KeyValueStore StoreBuilder Stores]
           [org.apache.kafka.common.serialization Serdes]))

(defprotocol KVStore
  (get-key [this k] "Gets the value for key")
  (set-key! [this k v] "Sets the value for key; deletes key when value is nil."))

(extend-type KeyValueStore
  KVStore
  (get-key [this k]
    (.get this (name k)))
  (set-key! [this k v]
    (.put this (name k) v)))

(defn state-store-builder
  "Returns a builder (for use with `.addStateStore`) for a persistent store with the specified name and serdes. Defaults to String serdes."
  ^StoreBuilder
  ([store-name key-serde value-serde]
   (Stores/keyValueStoreBuilder
    (Stores/persistentKeyValueStore store-name)
    key-serde
    value-serde))
  ([store-name]
   (state-store-builder store-name (Serdes/String) (Serdes/String))))

;;
;; A couple of convenience functions for getting/setting JSON string values.
;; These assume that the store was configured to use String serdes.
;; Not appropriate when using the actual JSON serdes.
;;

(defn get-json
  [kvstore k]
  (let [v (get-key kvstore k)]
    (and v (json/decode v true))))

(defn set-json!
  [kvstore k v]
  (set-key! kvstore k (and v (json/encode v))))

;;
;; Misc
;;

(defn get-all-kvs
  "Get _all_ the keys/values in a KeyValueStore. Handy for dev debugging, but be careful not to call it on a huge store."
  [^KeyValueStore state-store]
  (iterator-seq (.all state-store)))

;;
;; A mock implementation for tests that uses a map atom as a fake store.
;;
(defrecord MockKVStore [store]
  KVStore
  (get-key [this k]
    (get @store (keyword k)))
  (set-key! [this k v]
    (if v
      (swap! store assoc (keyword k) v)
      (swap! store dissoc (keyword k)))
    nil))

;; Convenience factory fn
(defn make-mock-store
  ([init-map]
   (->MockKVStore (atom init-map)))
  ([]
   (make-mock-store {})))

;; Mock helper
(defn get-mock-data
  [mock-store]
  @(:store mock-store))
