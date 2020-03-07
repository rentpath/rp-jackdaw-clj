(ns rp.jackdaw.topic-registry
  (:require [com.stuartsierra.component :as component]
            [rp.jackdaw.resolver :as resolver]
            [rp.jackdaw.schema :as schema]))

;; `topic-metadata` is a map of "unresolved" topic metadata.
(defrecord BaseTopicRegistry [topic-metadata serde-resolver-fn]
  component/Lifecycle
  (start [this]
    (assoc this
           :topic-configs
           (resolver/resolve-topics topic-metadata serde-resolver-fn)))
  (stop [this]
    this))

(defn resolve-schema
  [{:keys [schema-name schema-version]} schema-registry-url]
  (schema/fetch-schema schema-registry-url schema-name schema-version))


(defn resolve-schemata
  [topic-metadata schema-registry-url]
  (into {}
        (map (fn [topic-kw {:keys [topic-name key-serde value-serde] :as topic-config}]
               (let [{key-serde-keyword :serde-keyword} key-serde
                     {value-serde-keyword :serde-keyword} value-serde]
                 [topic-kw (-> topic-config
                               (= key-serde-keyword :jackdaw.serdes.avro.confluent/serde)
                               (assoc-in [:key-serde :schema] (resolve-schema key-serde schema-registry-url))
                               (= value-serde-keyword :jackdaw.serdes.avro.confluent/serde)
                               (assoc-in [:key-serde :schema] (resolve-schema value-serde schema-registry-url)))])))
        topic-metadata))

;;
;; Note that `type-registry` may be `nil` or left unspecified.
;; In such a case jackdaw's default type registry will be used.
;;

(defn map->TopicRegistry
  [{:keys [topic-metadata schema-registry-url type-registry]}]
  (map->BaseTopicRegistry {:topic-metadata    (resolve-schemata topic-metadata schema-registry-url)
                           :serde-resolver-fn (resolver/serde-resolver schema-registry-url type-registry)}))

(defn map->MockTopicRegistry
  [{:keys [topic-metadata type-registry]}]
  (map->BaseTopicRegistry {:topic-metadata topic-metadata
                           :serde-resolver-fn (resolver/mock-serde-resolver type-registry)}))
