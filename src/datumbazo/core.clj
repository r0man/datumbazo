(ns datumbazo.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join]]
            [sqlingvo.core :as sql]))

(defn as-identifier [table]
  (cond
   (keyword? table)
   (jdbc/as-identifier table)
   (and (map? table)
        (:name table))
   (->> (map table [:schema :name])
        (remove nil?)
        (map jdbc/as-identifier)
        (join "."))))

(defn count-rows
  "Count all rows in the database `table`."
  [table]
  (-> (sql/select '(count *))
      (sql/from table)
      (sql/run) first :count))

(defn delete-table
  "Delete all rows from the database `table`."
  [table & {:keys []}]
  (-> (str "DELETE FROM " (as-identifier table))
      (jdbc/do-commands)
      (first)))

(defn drop-table
  "Drop the database `table`."
  [table & opts]
  (-> (apply sql/drop-table table opts)
      (sql/run)
      (first)))

(defn truncate
  "Truncate the database `table`."
  [table & opts]
  (-> (apply sql/truncate table opts)
      (sql/run)
      (first)))

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

(defn select [table]
  (format "SELECT %s FROM %s"
          (if (empty? (:columns table))
            "*"
            (join ", " (map jdbc/as-identifier (:columns table))))
          (as-identifier table)))

(defn select-table [table & {:keys [page per-page]}]
  (jdbc/with-query-results rows
    [(str "SELECT * FROM " (as-identifier table))]
    (doall rows)))

(defn select-table-by-column [table column-name column-value & {:keys [page per-page]}]
  (jdbc/with-query-results rows
    [(str "SELECT * "
          "  FROM " (as-identifier table)
          " WHERE " (jdbc/as-identifier column-name) " = ?")
     column-value]
    (doall rows)))

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
           [& ~'opts] (apply drop-table ~(:name table#) ~'opts))

         (defn ~(symbol (str "delete-" table-name))
           ~(format "Delete all rows in the %s database table." table-name)
           [& ~'opts] (apply delete-table ~symbol# ~'opts))

         (defn ~(symbol (str "truncate-" table-name))
           ~(format "Truncate the %s database table." table-name)
           [& ~'opts] (apply truncate ~(:name table#) ~'opts))

         (defn ~table-name
           ~(format "Select %s from the database table." table-name)
           [& ~'opts] (apply select-table ~symbol# ~'opts))

         ~@(for [column# (map (comp symbol name) (:columns table#))]
             `(defn ~(symbol (str table-name "-by-" column#)) [~column# & ~'opts]
                (apply select-table-by-column ~symbol# ~(keyword column#) ~column# ~'opts))))))
