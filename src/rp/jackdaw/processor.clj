(ns rp.jackdaw.processor
  "Lightweight component wrapper around jackdaw Streams (DSL) processor app."
  (:require [com.stuartsierra.component :as component]
            [jackdaw.streams :as streams]
            [jackdaw.streams.mock :as mock])
  (:import [org.apache.kafka.streams KafkaStreams]))

;; This may be opinionated, but it seems like a saner default than the real default (which stops the app when a deserialization error occurs).
;; One can always override these defaults via custom app-config passed to the component.
(def app-config-defaults
  {"default.deserialization.exception.handler" "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler"})

;; Perform most of the component startup (up to building the topology).
;; From this point one could either start an actual streams app or create a toplogy test driver.
(defn- init-component
  [{:keys [topic-registry topology-builder-fn] :as component} streams-builder-fn]
   ;; Add topic-configs as a courtesy for the topology-builder-fn (one less layer to dig thru to get a specific topic config).
  (let [component (assoc component :topic-configs (:topic-configs topic-registry))
        builder (streams-builder-fn)
        topology ((topology-builder-fn component) builder)]
    (assoc component
           :builder builder
           :topology topology)))

;; Given an initialized processor component, create the Streams app and return it.
(defn- init-app
  [{:keys [topology app-config] :as component}]
  (streams/kafka-streams topology (merge app-config-defaults app-config)))

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
    (let [component (init-component this streams/streams-builder)
          app (init-app component)]
      (streams/start app)
      (assoc component :app app)))
  (stop [{:keys [app] :as this}]
    (when app
      (streams/close app))
    (dissoc this :app :builder :topology)))

(defn cleanup!
  "Delete the local state store directory. Will throw an exception when the Streams app is currently running, so best to call this before starting or after stopping."
  [component]
  (let [app (or (:app component)
                (init-app (init-component component streams/streams-builder)))]
    (.cleanUp ^KafkaStreams app)))

;;
;; A MockProcessor for unit testing a processor in isolation.
;;

(defrecord MockProcessor [topic-registry topology-builder-fn]
  component/Lifecycle
  (start [this]
    (let [component (init-component this mock/streams-builder)
          driver (mock/streams-builder->test-driver (:builder component))]
      (assoc component :driver driver)))
  (stop [this]
    (dissoc this :driver :builder :topology)))

;;
;; Helpers for working with a mock processor
;;

(defn mock-produce!
  "Produce a KV to a specific topic of a mock processor (with optional timestamp)."
  ([mock-processor topic-kw time-ms k v]
   (let [args [(:driver mock-processor)
               (get-in mock-processor [:topic-configs topic-kw])]
         args (cond-> args time-ms (conj time-ms))
         args (conj args k v)]
     (apply mock/publish args)))
  ([mock-processor topic-kw k v]
   (mock-produce! mock-processor topic-kw nil k v)))

(defn mock-get-keyvals
  "Get KV pairs from a specific output topic of a mock processor."
  [mock-processor topic-kw]
  (mock/get-keyvals (:driver mock-processor)
                    (get-in mock-processor [:topic-configs topic-kw])))

(comment
  (require '[rp.jackdaw.topic-registry :as registry])
  (require '[cheshire.core :as json])

  (def topic-metadata {:input
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

  ;; One can build arbitrarily complex topologies (with multiple input and/or output topics).
  ;; This example is a simple consumer-like topology that consumes messages from a single topic and performs some action for each record.
  (defn topology-builder-fn
    [{:keys [topic-configs] :as component}]
    (fn [builder]
      (-> (streams/kstream builder (:input topic-configs))
          (streams/for-each! (fn [[k v]] (clojure.pprint/pprint {:key k :value v}))))
      builder))

  (def sys (component/system-map
            :topic-registry (registry/map->TopicRegistry
                             {:topic-metadata topic-metadata
                              :schema-registry-url "http://localhost:8081"})
            :processor (component/using
                        (map->Processor
                         {:app-config {"application.id" "demo-processor"
                                       "bootstrap.servers" "localhost:9092"}
                          :topology-builder-fn topology-builder-fn})
                        [:topic-registry])))

  (def sys (component/start sys))
  ;; The processor is now running... watch stdout as it consumes the input topic.
  ;; If there aren't some records already in the topic, you could produce some however you like (for example using kafka-avro-console-producer or the Producer component from this lib).
  ;; A convenient option in the repl is to use the utility fns from rp.jackdaw.user like so...
  (require '[rp.jackdaw.user :as user])
  (user/produce! (user/producer-config)
                 (get-in sys [:topic-registry :topic-configs :input])
                 "some_key" {:x "Ahoy"})

  (def sys (component/stop sys))
  )
