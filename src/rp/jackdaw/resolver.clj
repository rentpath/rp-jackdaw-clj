(ns rp.jackdaw.resolver
  (:require [jackdaw.serdes.resolver :as resolver]
            [jackdaw.serdes.avro.schema-registry :as registry]))

;; Note: When `type-registry` is `nil`, it defaults to `jackdaw.serdes.avro/+base-schema-type-registry+`.

(defn serde-resolver
  "Return a resolver that uses the specified Schema Registry url."
  [& [schema-registry-url type-registry]]
  (resolver/serde-resolver :schema-registry-url schema-registry-url
                           :type-registry type-registry))

(defn mock-serde-resolver
  "Return a resolver suitable for use in tests that use jackdaw.streams.mock"
  [& [type-registry]]
  (resolver/serde-resolver :schema-registry-url "fake"
                           :schema-registry-client (registry/mock-client)
                           :type-registry type-registry))

;;
;; Helpers for resolving serdes inside topic metadata (or map thereof)
;;

(defn resolve-topic
  "Resolve serdes for a single topic."
  [topic-metadata resolve-serde]
  (-> topic-metadata
      ;; Currently (jackdaw 0.6.6) serde metadata requires a `:key?` boolean,
      ;; however it seems redundant to me to always have to specify that flag
      ;; as true for a `:key-serde` and as false for a `:value-serde`.
      ;; Let's set the flags as a courtesy for our users so they can omit them.
      (assoc-in [:key-serde :key?] true)
      (assoc-in [:value-serde :key?] false)
      (update :key-serde resolve-serde)
      (update :value-serde resolve-serde)))

(defn resolve-topics
  "Resolve serdes for a map of topics."
  [topics-metadata resolve-serde]
  (reduce-kv (fn [m k v]
               (assoc m k (resolve-topic v resolve-serde)))
             {}
             topics-metadata))
