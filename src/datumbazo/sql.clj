(ns datumbazo.sql
  (:refer-clojure :exclude [group-by replace])
  (:require [clojure.algo.monads :refer [m-seq state-m with-monad]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [datumbazo.util :as u]
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

;; PARSE SQL EXPRESSION

(defmulti parse-expr class)

(defn parse-fn-expr [expr]
  {:op :fn-call
   :name (first expr)
   :args (map parse-expr (rest expr))})

(defmethod parse-expr nil [expr]
  {:op :nil})

(defmethod parse-expr clojure.lang.Cons [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.PersistentList [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.IPersistentMap [expr]
  expr)

(defmethod parse-expr clojure.lang.Keyword [expr]
  (u/parse-column expr))

(defmethod parse-expr :default [expr]
  {:op :constant :form expr})

(defn- parse-expressions [expr-list]
  {:op :expr-list
   :children (map parse-expr (remove #(= * %1) expr-list))})

(defn as
  "Add an AS alias to the SQL statement."
  [statement alias]
  (assoc (parse-expr statement) :alias alias))

(defn continue-identity
  "Add the CONTINUE IDENTITY clause to the SQL statement."
  [continue-identity]
  (fn [statement]
    (let [node {:op :continue-identity :continue-identity continue-identity}]
      [node (assoc statement :continue-identity node)])))

(defn group-by
  "Add the GROUP BY clause to the SQL statement."
  [statement & expressions]
  (assoc statement
    :group-by {:op :group-by :expr-list (parse-expressions expressions)}))

(defn limit
  "Add the LIMIT clause to the SQL statement."
  [statement count]
  (assoc statement :limit {:op :limit :count count}))

(defn restart-identity
  "Add the RESTART IDENTITY clause to the SQL statement."
  [restart-identity]
  (fn [statement]
    (let [node {:op :restart-identity :restart-identity restart-identity}]
      [node (assoc statement :restart-identity node)])))

(defn if-exists
  "Add the IF EXISTS clause to the SQL statement."
  [if-exists]
  (fn [statement]
    (let [node {:op :if-exists :if-exists if-exists}]
      [node (assoc statement :if-exists node)])))

(defn offset
  "Add the OFFSET clause to the SQL statement."
  [statement start]
  (assoc statement :offset {:op :offset :start start}))

(defn order-by
  "Add the ORDER BY clause to the SQL statement."
  [statement expr-list & {:as opts}]
  (assoc statement
    :order-by
    (assoc opts
      :op :order-by
      :expr-list (parse-expressions (wrap-seq expr-list)))))

(defn from
  "Add the FROM item to the SQL select statement."
  [statement & from]
  (assoc statement
    :from {:op :from :from (map parse-from from)}))

(defn from
  "Add the FROM item to the SQL select statement."
  [statement & from]
  (assoc statement
    :from {:op :from :from (map parse-from from)}))

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

(defn drop-table
  "Drop the database `tables`."
  [tables & {:as opts}]
  (assoc opts
    :op :drop-table
    :tables (map table (wrap-seq tables))))

(defn truncate-table
  "Truncate the database `tables`."
  [tables & {:as opts}]
  (assoc opts
    :op :truncate-table
    :tables (map table (wrap-seq tables))))

(defn select
  "Select `expressions` from the database."
  [& expressions]
  {:op :select
   :expr-list (parse-expressions expressions)})