(ns database.sql
  (:refer-clojure :exclude [replace])
  (:require [clojure.algo.monads :refer [m-seq state-m with-monad]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace split]]
            [database.util :as u]
            [database.sql.compiler :refer :all])
  (:import database.sql.compiler.Table))

(defn cascade
  "Add the CASCADE clause to the SQL statement."
  [cascade?]
  (fn [statement]
    [cascade? (assoc statement :cascade? cascade?)]))

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

(defn drop-table
  "Add the RESTRICT clause to the SQL statement."
  [table & body]
  (second ((with-monad state-m (m-seq body))
           (->DropTable table false false false))))

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

;; (table
;;  :continents
;;  (column :id :serial))

;; (defn column [column & {:as options}]
;;   )

;; (column :public :continents :id)
;; (column :public.continents/id)

;; (drop-table
;;  :public.continents
;;  (restrict true)
;;  (if-exists false))

;; (defn table [name]
;;   (cond
;;    (keyword? name)
;;    (apply ->SQLTable nil nil)))

;; (table
;;  :public.continents
;;  (column :id :serial)
;;  (column :name :citext))

;; (compile-sql
;;  (second
;;   (drop-table
;;    (table
;;     :public.continents
;;     (column :id :serial)
;;     (column :name :citext))
;;    (restrict true)
;;    (if-exists false))))

;; ;; (jdbc/with-quoted-identifiers \"
;; ;;   (-> (drop-table :continents)
;; ;;       (restrict true)
;; ;;       (compile-sql)))

;; ;; (compile-sql (drop-table :public.continents :if-exists true))
;; ;; (compile-sql (drop-table :public.continents))
;; ;; (compile-sql :public.continents)

;; ;; ;; (str :public.continents/id)
