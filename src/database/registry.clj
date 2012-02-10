(ns database.registry
  (:use database.columns
        database.tables))

(defonce ^:dynamic *tables* (atom {}))

(defn find-table
  "Find the database table in *tables* by it's name."
  [name] (get @*tables* (table-keyword name)))

(defn register-table
  "Register the database table in *tables*."
  [table]
  (swap! *tables* assoc (table-keyword table) table)
  table)

(defmacro with-ensure-table
  "Evaluate body with a resolved `table`."
  [table & body]
  (let [table# table]
    `(if-let [~table# (find-table ~table#)]
       (do ~@body) (throw (Exception. "Table not found.")))))

(defmacro with-ensure-column
  "Evaluate body with a resolved `table` and `column`."
  [table column & body]
  (let [column# column table# table]
    `(with-ensure-table ~table#
       (let [~column# (if (column? ~column#) ~column# (get (:columns ~table#) ~column#))]
         (assert (column? ~column#))
         ~@body))))
