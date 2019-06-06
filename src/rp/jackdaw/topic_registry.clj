(ns rp.jackdaw.topic-registry
  (:require [com.stuartsierra.component :as component]
            [rp.jackdaw.resolver :as resolver]))

;; `topic-metadata` is a map of "unresolved" topic metadata.
;; `serde-resolver` is a `SerdeResolver` component.
(defrecord TopicRegistry [topic-metadata serde-resolver]
  component/Lifecycle
  (start [this]
    (assoc this :topic-configs (resolver/resolve-topics topic-metadata (:resolver serde-resolver))))
  (stop [this]
    this))
