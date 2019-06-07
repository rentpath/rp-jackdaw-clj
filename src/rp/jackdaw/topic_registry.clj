(ns rp.jackdaw.topic-registry
  (:require [com.stuartsierra.component :as component]
            [rp.jackdaw.resolver :as resolver]))

;; `topic-metadata` is a map of "unresolved" topic metadata.
(defrecord BaseTopicRegistry [topic-metadata serde-resolver-fn]
  component/Lifecycle
  (start [this]
    (assoc this
           :topic-configs
           (resolver/resolve-topics topic-metadata serde-resolver-fn)))
  (stop [this]
    this))

;;
;; Note that `type-registry` may be `nil` or left unspecified.
;; In such a case jackdaw's default type registry will be used.
;;

(defn map->TopicRegistry
  [{:keys [topic-metadata schema-registry-url type-registry]}]
  (map->BaseTopicRegistry {:topic-metadata topic-metadata
                           :serde-resolver-fn (resolver/serde-resolver schema-registry-url type-registry)}))

(defn map->MockTopicRegistry
  [{:keys [topic-metadata type-registry]}]
  (map->BaseTopicRegistry {:topic-metadata topic-metadata
                           :serde-resolver-fn (resolver/mock-serde-resolver type-registry)}))
