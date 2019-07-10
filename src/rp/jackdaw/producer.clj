(ns rp.jackdaw.producer
  "Lightweight component wrapper around jackdaw producer client."
  (:require [com.stuartsierra.component :as component]
            [jackdaw.client :as client])
  (:import [org.apache.kafka.clients.producer KafkaProducer]))

(defprotocol IProducer
  (produce!
    [this partition key value]
    [this key value]
    [this value]))

;; `producer-config` is a map of string KVs containing config properties.
;; See https://kafka.apache.org/documentation/#producerconfigs
;; At a minimum is must contain "bootstrap.servers".
;; For example:
;; {"bootstrap.servers" "localhost:9092"}
;; Note that there's no need to specify serializers in producer config
;; since serdes details are handled by the topic-registry.
;; `topic-registry` is an instance of `TopicRegistry`.
;; `topic-kw` is a keyword identifying the topic that messages will be produced to.
(defrecord Producer [producer-config topic-registry topic-kw]
  component/Lifecycle
  (start [this]
    (let [topic-config (get-in topic-registry [:topic-configs topic-kw])]
      (assert topic-config (str "Missing topic config for " (pr-str topic-kw)))
      (assoc this :topic-config topic-config
             :producer (client/producer producer-config topic-config))))
  (stop [{:keys [producer] :as this}]
    (when producer
      (.flush ^KafkaProducer producer)
      (.close ^KafkaProducer producer))
    (dissoc this :producer))

  IProducer
  ;; Note that client/produce! returns a future; if dereferenced, it will either return a map of metadata (on success) or throw an exception (on failure).
  ;; Sample metadata map:
  ;; `{:topic-name "my_topic"
  ;;   :partition 0
  ;;   :timestamp 1559758622503
  ;;   :offset 23
  ;;   :serialized-key-size 7
  ;;   :serialized-value-size 29}`
  (produce! [{:keys [producer topic-config]} value]
    (client/produce! producer topic-config value))
  (produce! [{:keys [producer topic-config]} key value]
    (client/produce! producer topic-config key value))
  (produce! [{:keys [producer topic-config]} partition key value]
    (client/produce! producer topic-config partition key value)))

;; A mock implementation for tests that keeps a record of all produce! calls in an atom.
(defrecord MockProducer [store]
  IProducer
  (produce! [this partition k v]
    (swap! store
           conj
           {:k k
            :v v
            :partition partition})
    ;; Return a future for consistency with the real producer, but don't bother trying to build a fake success map.
    (future nil))
  (produce! [this k v]
    (produce! this nil k v))
  (produce! [this v]
    (produce! this nil nil v)))

;; Convenience factory fn
(defn make-mock-producer
  []
  (->MockProducer (atom [])))

;; Mock helper
(defn get-mock-data
  [mock-producer]
  @(:store mock-producer))

(comment
  (require '[rp.jackdaw.topic-registry :as registry])
  (require '[cheshire.core :as json])

  (def topic-metadata {:target
                       {:topic-name "foo"
                        :partition-count 1
                        :replication-factor 1
                        :key-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                    :schema (json/encode "string")}
                        :value-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                      :schema (json/encode {:type "record"
                                                            :name "Demo"
                                                            :fields [{:name "x"
                                                                      :type "string"}]})}}})
  (def sys (component/system-map
            :topic-registry (registry/map->TopicRegistry
                             {:topic-metadata topic-metadata
                              :schema-registry-url "http://localhost:8081"})
            :producer (component/using
                       (map->Producer
                        {:producer-config {"bootstrap.servers" "localhost:9092"}
                         :topic-kw :target})
                       [:topic-registry])))
  (def sys (component/start sys))
  (def producer (:producer sys))
  @(produce! producer "a_key" {:x "Hello"})

  ;; Want to consume the message you just produced?
  ;; One convenient option in the repl is to use the utility fns from rp.jackdaw.user like so...
  (require '[rp.jackdaw.user :as user])
  (user/get-keyvals (user/consumer-config)
                    (get-in sys [:topic-registry :topic-configs :target]))

  (def sys (component/stop sys))
  )
