(ns database.meta
  (:require [clojure.java.jdbc :as jdbc]
            [database.protocol :refer [as-identifier as-keyword]]
            [inflections.core :refer [hyphenize]])
  (:import database.protocol.Nameable))

(defonce ^:dynamic *schemas*
  (atom {}))

(defn- register
  "Register `v` in `atom` under `ks` and and return `v`."
  [atom ks v] (swap! atom assoc-in ks v) v)

(defn- lookup
  "Lookup `ks` in `atom`."
  [atom ks] (get-in @atom ks))

;; SCHEMAS

(defrecord Schema [name]
  Nameable
  (as-keyword [schema]
    (jdbc/as-keyword (:name schema)))
  (as-identifier [schema]
    (jdbc/as-identifier (:name schema))))

(defn make-schema
  "Make a new database schema map."
  [name]
  (assert (keyword name) (str "Invalid schema name: " (prn-str name)))
  (Schema. (keyword (hyphenize name))))

(defn schema-key
  "Returns the lookup key for `schema` in *schema*"
  [schema] [(as-keyword schema)])

(defn register-schema
  "Register the database schema in *schemas*."
  [schema] (register *schemas* (schema-key schema) schema))

(defn lookup-schema
  "Lookup `schema` in *schemas*."
  [schema] (lookup *schemas* (schema-key schema)))

;; TABLES

;; COLUMNS


;; (defrecord Schema [name catalog])

;; (defn tables
;;   "Retrieves the schemas available to the database connection."
;;   [connection & {:keys [catalog schema-pattern table-pattern types]
;;                  :or {schema-pattern :public types [:table]}}]
;;   (->> (.getTables (.getMetaData connection)
;;                    (if catalog (name catalog))
;;                    (if schema-pattern (name schema-pattern))
;;                    (if table-pattern (name table-pattern))
;;                    (into-array String (map (comp upper-case name) (remove nil? types))))
;;        (resultset-seq)))

;; (defn info
;;   "Returns the product name, mayor and minor version of the database."
;;   [connection]
;;   (let [meta (.getMetaData connection)]
;;     {:product-name (.getDatabaseProductName meta)
;;      :major-version (.getDatabaseMajorVersion meta)
;;      :minor-version (.getDatabaseMinorVersion meta)}))

;; ;; (with-database :bs-database
;; ;;   (schemas (jdbc/find-connection)))

;; ;; (jdbc/connection)
