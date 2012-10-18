(ns database.sql
  (:refer-clojure :exclude [replace])
  (:require [clojure.algo.monads :refer [m-seq state-m with-monad]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [database.util :as u]
            [database.sql.compiler :refer [compile-sql]]))

(defn make-column
  "Make a database column."
  [table name type & {:as options}]
  (assoc  options
    :schema (:schema table)
    :table (:name table)
    :name name
    :type type))

(defn- wrap-seq [s]
  (if (sequential? s) s [s]))

(defn sql
  "Compile `statement` into a vector, where the first element is the
  SQL statement and the rest are the prepared statement arguments."
  [statement] (compile-sql statement))

;; PARSE SQL EXPRESSION

(defmulti parse-expr class)

(defn parse-fn-expr [expr]
  {:op :fn
   :name (first expr)
   :children (map parse-expr (rest expr))})

(defmethod parse-expr nil [expr]
  {:op :nil})

(defmethod parse-expr clojure.lang.Cons [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.PersistentList [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.Keyword [expr]
  {:op :keyword :form expr})

(defmethod parse-expr Number [expr]
  {:op :number :form expr})

(defmethod parse-expr String [expr]
  {:op :string :form expr})

(defn cascade
  "Add the CASCADE clause to the SQL statement."
  [cascade]
  (fn [statement]
    [cascade (assoc statement :cascade cascade)]))

(defn continue-identity
  "Add the CONTINUE IDENTITY clause to the SQL statement."
  [continue-identity]
  (fn [statement]
    [continue-identity (assoc statement :continue-identity true)]))

(defn limit
  "Add the LIMIT clause to the SQL statement."
  [count]
  (fn [statement]
    (let [limit {:op :limit :count count}]
      [limit (assoc statement :limit limit)])))

(defn restart-identity
  "Add the RESTART IDENTITY clause to the SQL statement."
  [restart-identity]
  (fn [statement]
    [restart-identity (assoc statement :restart-identity true)]))

(defn if-exists
  "Add the IF EXISTS clause to the SQL statement."
  [if-exists]
  (fn [statement]
    [if-exists (assoc statement :if-exists if-exists)]))

(defn offset
  "Add the OFFSET clause to the SQL statement."
  [start]
  (fn [statement]
    (let [offset {:op :offset :start start}]
      [offset (assoc statement :offset offset)])))

(defn restrict
  "Add the RESTRICT clause to the SQL statement."
  [restrict]
  (fn [statement]
    [restrict (assoc statement :restrict restrict)]))

(defn from
  "Add the FROM item to the SQL select statement."
  [from-item]
  (fn [statement]
    (let [from-item
          (map #(cond
                 (keyword? %1)
                 (assoc (u/parse-table %1)
                   :op :table))
               (wrap-seq from-item))]
      [from-item (assoc statement :from-item from-item)])))

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

(defmacro defstmt
  "Define a SQL statement."
  [name doc args & body]
  `(do (defn ~name ~doc
         [~@args & ~'body]
         (second ((with-monad state-m (m-seq ~'body))
                  ~@body)))))

(defstmt drop-table
  "Drop the database `table`."
  [tables]
  {:op :drop-table
   :children (map table (wrap-seq tables))})

(defstmt truncate-table
  "Truncate the database `table`."
  [tables]
  {:op :truncate-table
   :children (map table (wrap-seq tables))})

(defstmt select
  "Select from the database `table`."
  [expressions]
  {:op :select
   :expressions (map parse-expr (wrap-seq expressions))})
