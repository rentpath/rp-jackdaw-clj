(ns rp.jackdaw.producer
  "Lightweight component wrapper around jackdaw producer client."
  (:require [com.stuartsierra.component :as component]
            [jackdaw.client :as client]))

(defprotocol IProducer
  (produce!
    [this partition key value]
    [this key value]
    [this value]))

;; A mock implementation for tests that keeps a record of all produce! calls in an atom.
(defrecord MockProducer [store]
  IProducer
  (produce! [this partition k v]
    (swap! store
           conj
           {:k k
            :v v
            :partition partition}))
  (produce! [this k v]
    (produce! this nil k v))
  (produce! [this v]
    (produce! this nil nil v)))

;; Convenience factory fn
(defn mock-producer
  []
  (->MockProducer (atom [])))

;; Mock helper
(defn get-mock-store
  [producer]
  @(:store producer))


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
      (.flush ^Producer producer)
      (.close ^Producer producer))
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

(comment
  (require '[rp.jackdaw.topic-registry :as registry])
  (require '[rp.jackdaw.resolver :as resolver])
  (require '[cheshire.core :as json])

  (def topic-metadata {:topic-name "foo"
                       :partition-count 1
                       :replication-factor 1
                       :key-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                   :schema (json/encode "string")
                                   :key? true}
                       :value-serde {:serde-keyword :jackdaw.serdes.avro.confluent/serde
                                     :schema (json/encode {:type "record"
                                                           :name "Demo"
                                                           :fields [{:name "x"
                                                                     :type "string"}]})
                                     :key? false}})
  (def serde-resolver (resolver/map->SerdeResolver {:schema-registry-url "http://localhost:8081"}))
  (def serde-resolver (component/start serde-resolver))
  (def topic-registry (registry/map->TopicRegistry
                       {:serde-resolver serde-resolver
                        :topic-metadata {:target topic-metadata}}))
  (def topic-registry (component/start topic-registry))
  (def producer (map->Producer
                 {:producer-config {"bootstrap.servers" "localhost:9092"}
                  :topic-registry topic-registry
                  :topic-kw :target}))
  (def producer (component/start producer))
  @(produce! producer "a_key" {:x "Hello"})
  (def producer (component/stop producer))
)
