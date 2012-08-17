(ns database.registry
  (:use database.protocol))

(defonce ^:dynamic *schemas*
  (atom {}))

(defonce ^:dynamic *tables*
  (atom {}))

(defonce ^:dynamic *columns*
  (atom {}))

(defn schema [name]
  (get @*schemas* (as-keyword name)))

(defn table
  ([table]
     (database.registry/table :public table))
  ([schema table]
     (get-in @*tables* [(as-keyword schema) (as-keyword table)])))

(defn column
  ([table column]
     (database.registry/column :public table column))
  ([schema table column]
     (get-in @*schemas*
             [(as-keyword schema)
              (as-keyword table)
              (as-keyword column)])))

(defn schema-key
  "Returns the lookup key for `schema` in *tables*."
  [schema] [(as-keyword schema)])

(defn table-key
  "Returns the lookup key for `table` in *tables*."
  [table] [(as-keyword (or (:schema table) :public))
           (as-keyword (:name table))])

(defn column-key
  "Returns the lookup key for `column` in *columns*."
  [column] [(as-keyword (or (:schema column) :public))
            (as-keyword (:table column))
            (as-keyword (:name column))])

(defn- register [atom v key-fn]
  (swap! atom assoc-in (key-fn v) v)
  v)

(defn register-schema
  "Register the database schema in *schemas*."
  [schema] (register *schemas* schema schema-key))

(defn register-table
  "Register the database table in *tables*."
  [table] (register *tables* table table-key))

(defn register-column
  "Register the database column in *columns*."
  [column] (register *columns* column column-key))

;; (defn registry/table
;;   "Find the database table in *tables* by it's name."
;;   [table]
;;   (if (table? table)
;;     table (get @*tables* (keyword (qualified-table-name table)))))

;; (defn register-table
;;   "Register the database table in *tables*."
;;   [table]
;;   (swap! *tables* assoc (keyword (qualified-table-name table)) table)
;;   table)

(defmacro with-ensure-table
  "Evaluate body with a resolved `table`."
  [[sym table] & body]
  (let [table# table]
    `(if-let [~sym (registry/table ~table#)]
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
