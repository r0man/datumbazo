(ns database.core
  (:require [clojure.algo.monads :refer [m-seq state-m with-monad]]
            [clojure.java.jdbc :as jdbc]
            [environ.core :refer [env]]))

(defmacro with-connection
  "Evaluates body in the context of a new connection to the `name`
  database. The connection spec for `name` is looked up via environ."
  [name & body]
  `(let [name# ~name]
     (jdbc/with-connection
       (if (keyword? name#)
         (or (env name#)
             (throw (IllegalArgumentException. (format "Can't resolve connection spec via environ: %s" name#))))
         name#)
       ~@body)))

(defn make-table [name & {:as options}]
  (assoc options
    :name (keyword name)))

(defn make-column [name type & {:as options}]
  (assoc options
    :name (keyword name)))

(defn column [table name type & options]
  (let [column (apply make-column name type options)]
    (-> (update-in table [:columns] #(concat %1 [(:name column)]))
        (assoc-in [:column (:name column)] column))))

(defn schema [table schema]
  (assoc table :schema schema))

(defn register-table [table]
  table)

(defmacro deftable
  "Compose multiple fields into one field-like item."
  [name doc & body]
  `(def ~name
     (register-table
      (-> (make-table ~(keyword name) :doc ~doc)
          ~@body))))

;; (deftable continents)
;; (continents :x 1)

;; (deftable countries
;;   (column :id :serial)
;;   (column :continent-id :integer :references :continents/id)
;;   (column :name))
