(ns database.registry
  (:use database.columns
        database.tables))

(defonce ^:dynamic *tables* (atom {}))

(defn find-table
  "Find the database table in *tables* by it's name."
  [table]
  (if (table? table)
    table (get @*tables* (keyword (qualified-table-name table)))))

(defn register-table
  "Register the database table in *tables*."
  [table]
  (swap! *tables* assoc (keyword (qualified-table-name table)) table)
  table)

(defmacro with-ensure-table
  "Evaluate body with a resolved `table`."
  [[sym table] & body]
  (let [table# table]
    `(if-let [~sym (find-table ~table#)]
       (do ~@body) (throw (Exception. (str "Table not found: " ~table#))))))

(defmacro with-ensure-column
  "Evaluate body with a resolved `table` and `column`."
  [[table [sym column]] & body]
  (let [column# column table# (gensym)]
    `(with-ensure-table [~table# ~table]
       (let [~sym (if (column? ~column#) ~column# (get (:columns ~table#) ~column#))
             ~sym (assoc ~sym :table ~table#)]
         (assert (column? ~sym))
         ~@body))))
