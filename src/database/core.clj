(ns database.core
  (:require [clojure.java.jdbc :as jdbc]))

(defn count-rows
  "Count all rows in the database `table`."
  [table]
  (jdbc/with-query-results rows
    [(str "SELECT count(*) FROM " (jdbc/as-identifier table))]
    (:count (first (doall rows)))))

(defn delete-table
  "Delete all rows from the database `table`."
  [table] (-> (jdbc/do-commands (str "DELETE FROM " (jdbc/as-identifier table)))
              (first)))

(defn truncate-table
  "Truncate the database `table`."
  [table & {:keys [cascade continue-identity restart-identity restrict]}]
  (-> (str "TRUNCATE TABLE " (jdbc/as-identifier table)
           (if cascade " CASCADE")
           (if continue-identity "CONTINUE IDENTITY")
           (if restart-identity "RESTART IDENTITY")
           (if restrict " RESTRICT"))
      (jdbc/do-commands)
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

(defn register-table
  "Register a database table."
  [table] table)

(defmacro deftable
  "Define a database table."
  [name doc & body]
  `(do (def ~name
         (register-table
          (-> (make-table ~(keyword name) :doc ~doc)
              ~@body)))
       (defn ~(symbol (str "truncate-" name))
         ~(format "Truncate the database table %s." (keyword name))
         [& ~'opts]
         (apply truncate-table ~(keyword name) ~'opts))))

(defmacro deftable
  "Define a database table."
  [name doc & body]
  `(do (def ~name
         (register-table
          (-> (make-table ~(keyword name) :doc ~doc)
              ~@body)))
       (defn ~(symbol (str "delete-" name))
         ~(format "Delete all rows in the database table %s." (keyword name))
         [& ~'opts]
         (apply delete-table ~(keyword name) ~'opts))
       (defn ~(symbol (str "truncate-" name))
         ~(format "Truncate the database table %s." (keyword name))
         [& ~'opts]
         (apply truncate-table ~(keyword name) ~'opts))))
