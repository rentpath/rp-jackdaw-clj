(ns rp.jackdaw.processor
  "Lightweight component wrapper around jackdaw Streams (DSL) processor app."
  (:require [com.stuartsierra.component :as component]
            [jackdaw.streams :as streams]))

(def app-config-defaults
  {"default.deserialization.exception.handler" "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler"})

;; `app-config` is a map of string KVs containing config properties.
;; See https://kafka.apache.org/documentation/#streamsconfigs
;; At a minimum it must contain "application.id" and "bootstrap.servers".
;; (Note that the specified config is merged onto `app-config-defaults`.)
;; `topic-registry` is an instance of `TopicRegistry`.
;; `topology-builder-fn` takes the component as its only arg and returns a function that takes a streams builder; the returned function should build the topology and return the builder.
;; The component arg can be used to access the topic-configs or topic-registry and any other sub-values of the component (such as component dependencies).
(defrecord Processor [app-config topic-registry topology-builder-fn]
  component/Lifecycle
  (start [this]
    ;; Add topic-configs as a courtesy for the topology-builder-fn (one less layer to dig thru to get a specific topic config).
    (let [component (assoc this :topic-configs (:topic-configs topic-registry))
          builder (streams/streams-builder)
          topology ((topology-builder-fn component) builder)
          app (streams/kafka-streams topology (merge app-config-defaults app-config))]
      (streams/start app)
      (assoc component :app app)))
  (stop [{:keys [app] :as this}]
    (when app
      (streams/close app))
    (dissoc this :app)))

(comment
  (require '[rp.jackdaw.topic-registry :as registry])
  (require '[rp.jackdaw.resolver :as resolver])
  (require '[cheshire.core :as json])

  (def topic-metadata {:input
                       {:topic-name "foo"
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
                                      :key? false}}})
  (def serde-resolver (resolver/map->SerdeResolver {:schema-registry-url "http://localhost:8081"}))
  (def serde-resolver (component/start serde-resolver))
  (def topic-registry (registry/map->TopicRegistry
                       {:serde-resolver serde-resolver
                        :topic-metadata topic-metadata}))
  (def topic-registry (component/start topic-registry))

  ;; One can build arbitrarily complex topologies (with multiple input and/or output topics).
  ;; This example is a simple consumer-like topology that consumes messages from a single topic and performs some action for each record.
  (defn topology-builder-fn
    [{:keys [topic-configs] :as component}]
    (fn [builder]
      (-> (streams/kstream builder (:input topic-configs))
          (streams/for-each! (fn [[k v]] (clojure.pprint/pprint {:key k :value v}))))
      builder))

  (def processor (map->Processor
                  {:app-config {"application.id" "demo-processor"
                                "bootstrap.servers" "localhost:9092"}
                   :topic-registry topic-registry
                   :topology-builder-fn topology-builder-fn}))
  (def processor (component/start processor))
  (def processor (component/stop processor))
)
