(ns database.core
  (:require [clojure.java.jdbc :as jdbc]))

(defn make-table
  "Make a database table."
  [name & {:as options}]
  (assoc options
    :name (keyword name)))

(defn make-column
  "Make a database column."
  [name type & {:as options}]
  (assoc options
    :name (keyword name)
    :type type))

(defn column
  "Add a database column to `table`."
  [table name type & options]
  (let [column (apply make-column name type options)]
    (-> (update-in table [:columns] #(concat %1 [(:name column)]))
        (assoc-in [:column (:name column)] column))))

(defn schema
  "Assoc `schema` under the :schema key to `table`."
  [table schema] (assoc table :schema schema))

(defn register-table
  "Register a database table."
  [table] table)

(defmacro deftable
  "Define a database table."
  [name doc & body]
  `(def ~name
     (register-table
      (-> (make-table ~(keyword name) :doc ~doc)
          ~@body))))
