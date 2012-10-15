(ns database.sql
  (:refer-clojure :exclude [replace])
  (:require [clojure.algo.monads :refer [m-seq state-m with-monad]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [database.util :as u]
            [database.sql.compiler :refer :all])
  (:import database.sql.compiler.Table))

(defn- wrap-sequential [s]
  (if (sequential? s) s [s]))

(defn sql
  "Compile `statement` into a vector, where the first element is the
  SQL statement and the rest are the prepared statement arguments."
  [statement] (compile-sql statement))

(defmulti parse-expr class)

(defn parse-fn-expr [expr]
  {:op :fn
   :form (first expr)
   :children (map parse-expr (rest expr))})

(defmethod parse-expr clojure.lang.Cons [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.PersistentList [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.Keyword [expr]
  {:op :keyword :form expr})

(defmethod parse-expr Number [expr]
  {:op :constant :form expr})

(defmethod parse-expr String [expr]
  {:op :string :form expr})

(declare expand-sql)

(defn- expand-fn [arg]
  (str (name (first arg)) "("  (join ", " (map expand-sql (rest arg))) ")"))

(defn expand-sql
  "Expand `arg` into an SQL statement."
  [arg]
  (cond
   (symbol? arg)
   (name arg)
   (instance? clojure.lang.Cons arg)
   (expand-fn arg)
   (string? arg)
   (str \" arg \")
   (keyword? arg)
   (jdbc/as-identifier arg)
   (list? arg)
   (expand-fn arg)
   :else (.toUpperCase (str arg))))

(defn cascade
  "Add the CASCADE clause to the SQL statement."
  [cascade?]
  (fn [statement]
    [cascade? (assoc statement :cascade? cascade?)]))

(defn continue-identity
  "Add the CONTINUE IDENTITY clause to the SQL statement."
  [continue-identity?]
  (fn [statement]
    [continue-identity? (assoc statement :continue-identity? true)]))

(defn restart-identity
  "Add the RESTART IDENTITY clause to the SQL statement."
  [restart-identity?]
  (fn [statement]
    [restart-identity? (assoc statement :restart-identity? true)]))

(defn if-exists
  "Add the IF EXISTS clause to the SQL statement."
  [if-exists?]
  (fn [statement]
    [if-exists? (assoc statement :if-exists? if-exists?)]))

(defn restrict
  "Add the RESTRICT clause to the SQL statement."
  [restrict?]
  (fn [statement]
    [restrict? (assoc statement :restrict? restrict?)]))

(defn from
  "Add the FROM item to the SQL select statement."
  [from-item]
  (fn [statement]
    (let [from-item (wrap-sequential from-item)]
      [from-item (assoc statement :from from-item)])))

(defn table
  "Make a SQL table."
  [table & body]
  (second ((with-monad state-m (m-seq body))
           (to-table table))))

(defn column
  "Make a SQL table."
  [name type & options]
  (fn [table]
    (assert (table? table))
    (let [column (apply make-column table name type options)]
      [column (assoc-in table [:column name] column)])))

(defmacro deftable
  "Define a database table."
  [symbol doc name & body]
  (let [table# (eval `(-> (table ~name ~@body)))]
    (prn table#)
    `(def ^{:doc doc}
       ~symbol (table ~name ~@body))))

(defmacro defstmt
  "Define a SQL statement."
  [name doc args & body]
  `(do (defn ~name ~doc
         [~@args & ~'body]
         (second ((with-monad state-m (m-seq ~'body))
                  ~@body)))))

;; (defn drop-table
;;   "Add the RESTRICT clause to the SQL statement."
;;   [table & body]
;;   (second ((with-monad state-m (m-seq body))
;;            (->DropTable (to-table table) false false false))))

(defstmt drop-table
  "Drop the database `table`."
  [tables]
  (->DropTable
   (map to-table (wrap-sequential tables))
   false false false))

(defstmt truncate-table
  "Truncate the database `table`."
  [tables]
  (->TruncateTable
   (map to-table (wrap-sequential tables))
   false false false false))

(defstmt select
  "Select from the database `table`."
  [columns] (->Select (wrap-sequential columns) nil))

(defstmt select
  "Select from the database `table`."
  [columns]
  (->Select (map expand-sql (wrap-sequential columns)) nil))

;; (sql
;;  (select
;;   [:id :name]
;;   (from :continents)))

;; (select
;;  [:countries.id :countires.name]
;;  (from :countries)
;;  (join :continents
;;        (on (= :continents.id :countires.continent-id))))

;; (deftable continents
;;   "The continents database table."
;;   :continents
;;   (column :id :serial)
;;   (column :name :text :not-null? true :unique? true)
;;   (column :code :varchar :length 2 :not-null? true :unique? true)
;;   (column :geometry :geometry)
;;   (column :freebase-guid :text :unique? true)
;;   (column :geonames-id :integer :unique? true)
;;   (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
;;   (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))

;; (table
;;  :public.continents
;;  (column :id :serial)
;;  (column :name :text :not-null? true :unique? true)
;;  (column :code :varchar :length 2 :not-null? true :unique? true)
;;  (column :geometry :geometry)
;;  (column :freebase-guid :text :unique? true)
;;  (column :geonames-id :integer :unique? true)
;;  (column :created-at :timestamp-with-time-zone :not-null? true :default "now()")
;;  (column :updated-at :timestamp-with-time-zone :not-null? true :default "now()"))


;; (select
;;  [:id :name]
;;  (from :continents))

;; (select
;;  [:countries.id :countries.name]
;;  (from [:countries :continents])
;;  (where `(and (= :countries.contient-id :continents.id)
;;               (> :countries.spot-count 0))))

;; (parse-expr
;;  `(and (= :countries.contient-id :continents.id)
;;        (> :countries.spot-count 0)))

;; (parse-expr `(> :countries.spot-count 0))

;; (parse-expr 1.2)

;; (name (first `(and (= :countries.contient-id :continents.id)
;;                    (> :countries.spot-count 0))))

;; ;; SELECT max(temp_lo) FROM weather;

;; (select
;;  ['(max :temp-lo)]
;;  (from :weather))

;; (defn foo [a]
;;   (select
;;    `[:a (public.max ~a)]
;;    (from :weather)))

;; (println (expand-sql '(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))))
;; (println (expand-sql `(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))))

;; (prn (expand-sql '(max 1 2 3 :a)))
;; ST_AsText(ST_Centroid('MULTIPOINT ( -1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6 )'));

;; (jdbc/with-quoted-identifiers \"
;;   (expand-sql '(:max 1 2 3 :a)))

;; (let [a 1]
;;   (select
;;    `(:max ~a)
;;    (from :weather)))
