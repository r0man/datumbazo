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
  [name type & {:as opts}]
  (fn [table]
    (assert (instance? Table table))
    (let [column (assoc (map->Column opts)
                   :schema (:schema table)
                   :table (:name table)
                   :name name
                   :type type)]
      [column (assoc-in table [:column name] column)])))

;; (table
;;  :continents
;;  (column :id :serial :primary-key? true))

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
