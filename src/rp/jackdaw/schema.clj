(ns rp.jackdaw.schema
  (:require [cheshire.core :as json]))

;; Nothing really jackdaw-specific here.

;; Utility that may be helpful when defining topic config serdes and using the Avro Schema Registry.
(defn fetch-schema
  "Pull a specific version (like `\"1\"`, `\"2\"` or `\"latest\"`) of a schema from the registry.
  Returns a string containing the AVSC (JSON). Suitable for use as the `:schema` value for serde metadata."
  [registry-url subject version]
  (-> (format "%s/subjects/%s/versions/%s" registry-url subject version)
      slurp
      (json/parse-string true)
      :schema))
