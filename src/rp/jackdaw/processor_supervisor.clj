(ns rp.jackdaw.processor-supervisor
  (:require [com.stuartsierra.component :as component]
            [rp.jackdaw.processor :as processor]
            [clojure.tools.logging :as log]))

(defprotocol Supervisor
  (handle-thread-exception [this thread throwable]
    "Return truthy value when willing and able to handle the exception.
     Supervisors should only attempt to handle an exception
     if the thread is associated with the supervised processor."))

(defn- thread-names
  [processor]
  (set (map #(.threadName %)
            (.localThreadsMetadata (:app processor)))))

(defn- supervisor-state
  [processor topic-registry]
  {:processor processor
   :thread-names (thread-names processor)
   :topic-registry topic-registry})

(defn- maybe-handle-thread-exception
  [{:keys [state-atom maybe-update-topic-registry] :as this} ex-thread throwable]
  (let [{:keys [processor thread-names topic-registry]} @state-atom
        ex-thread-name (.getName ex-thread)]
    (when (contains? thread-names ex-thread-name)
      (log/info "Attempting to handle thread exception..." {:thread-name ex-thread-name})
      (when-let [new-topic-registry (maybe-update-topic-registry topic-registry processor)]
        (let [new-processor (component/start
                             (processor/map->Processor
                              (assoc processor :topic-registry new-topic-registry)))]
          (reset! state-atom (supervisor-state new-processor new-topic-registry))
          (log/info "Successfully started new processor with new output schema!")
          ;; Return true to indicate success
          true)))))

(defrecord ProcessorSupervisor [processor topic-registry maybe-update-topic-registry]
  component/Lifecycle
  (start [{:keys [processor topic-registry] :as this}]
    (assoc this
           :state-atom
           (atom (supervisor-state processor topic-registry))))
  (stop [{:keys [state-atom] :as this}]
    (when-let [processor (and state-atom (:processor @state-atom))]
      (try
        (component/stop processor)
        (catch Throwable t
          (log/error t "Caught exception stopping processor"))))
    (dissoc this :state-atom))

  Supervisor
  (handle-thread-exception [this thread throwable]
    (maybe-handle-thread-exception this thread throwable)))
