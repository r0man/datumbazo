(ns datumbazo.core
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join]]
            [datumbazo.meta :as meta]
            [datumbazo.io :as io]
            [inflections.core :refer [singular]]
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

(defn count-all
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
  (first (sql/run (apply sql/drop-table table opts))))

(defn truncate
  "Truncate the database `table`."
  [table & opts]
  (first (sql/run (apply sql/truncate table opts))))

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

(defn select-table [table & {:keys [page per-page]}]
  (-> (sql/select *)
      (sql/from table)
      (sql/run)))

(defn select-table-by-column [table column-name column-value & {:keys [page per-page]}]
  (-> (sql/select *)
      (sql/from table)
      (sql/where `(= ~column-name ~column-value))
      (sql/run)))

(defn insert
  "Insert `row` into the database `table`."
  [table row]
  (-> (sql/insert table (map #(io/encode-row table %1) (if (sequential? row) row [row])))
      (sql/returning *)
      (sql/run)))

(defn update
  "Update `row` to the database `table`."
  [table row]
  (let [pks (meta/primary-keys (jdbc/connection) :table table)
        keys (map :name pks)
        vals (map row keys)]
    (-> (sql/update table (io/encode-row table row))
        (sql/where (list 'and (mapcat #(list '= %1 %2) keys vals)))
        (sql/returning *)
        (sql/run))))

(defn save
  "Save `row` to the database `table`."
  [table row]
  (or (first (update table row))
      (first (insert table row))))

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

         (defn ~(symbol (str "insert-" (singular (str table-name))))
           ~(format "Insert the %s row into the database." (singular (str table-name)))
           [~'row & ~'opts] (first (apply insert ~(:name table#) ~'row ~'opts)))

         (defn ~(symbol (str "save-" (singular (str table-name))))
           ~(format "Save the %s row to the database." (singular (str table-name)))
           [~'row & ~'opts] (apply save ~(:name table#) ~'row ~'opts))

         (defn ~(symbol (str "truncate-" table-name))
           ~(format "Truncate the %s database table." table-name)
           [& ~'opts] (apply truncate ~(:name table#) ~'opts))

         (defn ~(symbol (str "update-" (singular (str table-name))))
           ~(format "Update the %s row in the database." (singular (str table-name)))
           [~'row & ~'opts] (first (apply update ~(:name table#) ~'row ~'opts)))

         (defn ~table-name
           ~(format "Select %s from the database table." table-name)
           [& ~'opts] (apply select-table ~(:name table#) ~'opts))

         ~@(for [column# (map (comp symbol name) (:columns table#))]
             `(defn ~(symbol (str table-name "-by-" column#)) [~column# & ~'opts]
                (apply select-table-by-column ~(:name table#) ~(keyword column#) ~column# ~'opts))))))
