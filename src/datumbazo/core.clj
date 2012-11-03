(ns datumbazo.core
  (:refer-clojure :exclude [group-by])
  (:require [clojure.java.jdbc :as jdbc]
            [datumbazo.connection :as connection]
            [datumbazo.io :as io]
            [datumbazo.meta :as meta]
            [datumbazo.util :refer [immigrate]]
            [inflections.core :refer [singular]]))

(immigrate 'sqlingvo.core)

(defmacro with-connection [db-name & body]
  `(connection/with-connection ~db-name ~@body))

(defn run
  "Run the SQL statement `stmt`."
  [stmt]
  (->> (sqlingvo.core/run stmt)
       (map io/decode-row)
       (doall)))

(defn count-all
  "Count all rows in the database `table`."
  [table]
  (-> (select '(count *))
      (from table)
      (run) first :count))

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

(defn insert
  "Insert `row` into the database `table`."
  [table row]
  (let [rows (if (sequential? row) row [row])]
    (-> (sqlingvo.core/insert
         table (io/encode-rows table rows))
        (returning *))))

(defn insert
  "Insert rows into the database `table`."
  [table what]
  (if (sequential? what)
    (-> (sqlingvo.core/insert table (io/encode-rows table what))
        (returning *))
    (-> (sqlingvo.core/insert table what)
        (returning *))))

(defn update
  "Update `row` to the database `table`."
  [table row]
  (let [pks (meta/unique-columns (jdbc/connection) :table table)
        keys (map :name pks)
        vals (map row keys)]
    (-> (sqlingvo.core/update table (io/encode-row table row))
        (where (cons 'or (map #(list '= %1 %2) keys vals)))
        (returning *))))

(defmacro deftable
  "Define a database table."
  [table-name doc & body]
  (let [table# (eval `(-> (make-table ~(keyword table-name) :doc ~doc) ~@body))
        symbol# (symbol (str table-name "-table"))]
    `(do (def ~symbol#
           (-> (make-table ~(keyword table-name) :doc ~doc)
               ~@body))

         (defn ~(symbol (str "drop-" table-name))
           ~(format "Drop the %s database table." table-name)
           [& ~'opts]
           (-> (apply drop-table ~(:name table#) ~'opts)
               run first :count))

         (defn ~(symbol (str "delete-" table-name))
           ~(format "Delete all rows in the %s database table." table-name)
           [& ~'opts]
           (-> (str "DELETE FROM " (jdbc/as-identifier ~(:name table#)))
               (jdbc/do-commands)
               first))

         (defn ~(symbol (str "insert-" (singular (str table-name))))
           ~(format "Insert the %s row into the database." (singular (str table-name)))
           [~'row & ~'opts] (first (run (apply insert ~(:name table#) [~'row] ~'opts))))

         (defn ~(symbol (str "insert-" (str table-name)))
           ~(format "Insert the %s rows into the database." (singular (str table-name)))
           [~'rows & ~'opts] (run (apply insert ~(:name table#) ~'rows ~'opts)))

         (defn ~(symbol (str "truncate-" table-name))
           ~(format "Truncate the %s database table." table-name)
           [& ~'opts]
           (-> (apply truncate ~(:name table#) ~'opts)
               run first :count))

         (defn ~(symbol (str "update-" (singular (str table-name))))
           ~(format "Update the %s row in the database." (singular (str table-name)))
           [~'row & ~'opts] (first (run (apply update ~(:name table#) ~'row ~'opts))))

         (defn ~(symbol (str "save-" (singular (str table-name))))
           ~(format "Save the %s row to the database." (singular (str table-name)))
           [~'row & ~'opts]
           (or (apply ~(symbol (str "update-" (singular (str table-name)))) ~'row ~'opts)
               (apply ~(symbol (str "insert-" (singular (str table-name)))) ~'row ~'opts)))

         (defn ~table-name
           ~(format "Select %s from the database table." table-name)
           [& ~'opts]
           (-> (select *)
               (from ~(:name table#))
               (run)))

         ~@(for [column# (map (comp symbol name) (:columns table#))]
             `(defn ~(symbol (str table-name "-by-" column#)) [~'value & ~'opts]
                (let [column# (first (meta/columns (jdbc/connection) :table ~(:name table#) :name ~(keyword column#)))]
                  (assert column#)
                  (-> (select *)
                      (from ~(:name table#))
                      (where `(= ~(:name column#) ~(io/encode-column column# ~'value)))
                      (run))))))))
