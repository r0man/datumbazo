(ns datumbazo.sql
  (:refer-clojure :exclude [group-by replace])
  (:require [clojure.algo.monads :refer [m-seq state-m with-monad]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [datumbazo.util :as u]
            [datumbazo.sql.expr :refer [parse-expr parse-exprs]]
            [datumbazo.sql.compiler :refer [compile-sql]]))

(defn make-column
  "Make a database column."
  [table name type & {:as options}]
  (assoc  options
    :schema (:schema table)
    :table (:name table)
    :name name
    :type type))

(defn- parse-from [forms]
  (cond
   (keyword? forms)
   (u/parse-table forms)
   (and (map? forms) (= :select (:op forms)))
   forms
   :else (throw (IllegalArgumentException. (str "Can't parse FROM form: " forms)))))

(defn- wrap-seq [s]
  (if (sequential? s) s [s]))

(defn sql
  "Compile `statement` into a vector, where the first element is the
  SQL statement and the rest are the prepared statement arguments."
  [statement] (compile-sql statement))

(defn as
  "Add an AS clause to the SQL statement."
  [statement as]
  (assoc (parse-expr statement) :as as))

(defn drop-table
  "Drop the database `tables`."
  [tables & {:as opts}]
  (assoc opts
    :op :drop-table
    :tables (map u/parse-table (wrap-seq tables))))

(defn from
  "Add the FROM item to the SQL select statement."
  [statement & from]
  (assoc statement
    :from {:op :from :from (map parse-from from)}))

(defn group-by
  "Add the GROUP BY clause to the SQL statement."
  [statement & exprs]
  (assoc statement
    :group-by {:op :group-by :exprs (parse-exprs exprs)}))

(defn limit
  "Add the LIMIT clause to the SQL statement."
  [statement count]
  (assoc statement :limit {:op :limit :count count}))

(defn offset
  "Add the OFFSET clause to the SQL statement."
  [statement start]
  (assoc statement :offset {:op :offset :start start}))

(defn order-by
  "Add the ORDER BY clause to the SQL statement."
  [statement exprs & {:as opts}]
  (assoc statement
    :order-by
    (assoc opts
      :op :order-by
      :exprs (parse-exprs (wrap-seq exprs)))))

(defn table
  "Make a SQL table."
  [table & body]
  (second ((with-monad state-m (m-seq body))
           (assoc (u/parse-table table)
             :op :table))))

(defn column
  "Make a SQL table."
  [name type & options]
  (fn [table]
    (let [column (apply make-column table name type options)]
      [column (assoc-in table [:column name] column)])))

(defmacro deftable
  "Define a database table."
  [symbol doc name & body]
  (let [table# (eval `(-> (table ~name ~@body)))]
    `(def ^{:doc doc}
       ~symbol (table ~name ~@body))))

(defn select
  "Select `exprs` from the database."
  [& exprs]
  {:op :select :exprs (parse-exprs exprs)})

(defn truncate-table
  "Truncate the database `tables`."
  [tables & {:as opts}]
  (assoc opts
    :op :truncate-table
    :tables (map table (wrap-seq tables))))

(defn union
  "Add the UNION clause to the SQL statement."
  [stmt-1 stmt-2 & {:keys [all]}]
  {:op :union :children [stmt-1 stmt-2] :all all})

(defn where
  "Add the WHERE clause to the SQL statement."
  [statement condition]
  (assoc statement
    :condition
    {:op :condition
     :condition (parse-expr condition)}))
