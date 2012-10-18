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
    (let [node {:op :cascade :cascade cascade}]
      [node (assoc statement :cascade node)])))

(defn continue-identity
  "Add the CONTINUE IDENTITY clause to the SQL statement."
  [continue-identity]
  (fn [statement]
    (let [node {:op :continue-identity :continue-identity continue-identity}]
      [node (assoc statement :continue-identity node)])))

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
  [start]
  (fn [statement]
    (let [node {:op :offset :start start}]
      [node (assoc statement :offset node)])))

(defn restrict
  "Add the RESTRICT clause to the SQL statement."
  [restrict]
  (fn [statement]
    (let [node {:op :restrict :restrict restrict}]
      [node (assoc statement :restrict node)])))

(defn from
  "Add the FROM item to the SQL select statement."
  [from]
  (fn [statement]
    (let [from
          (map #(cond
                 (keyword? %1)
                 (assoc (u/parse-table %1)
                   :op :table))
               (wrap-seq from))]
      [from (assoc statement :from from)])))

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
