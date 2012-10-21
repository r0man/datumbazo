(ns datumbazo.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? join replace]]))

(defmulti compile-sql :op)

(defn stmt? [arg]
  (and (sequential? arg) (string? (first arg))))

(defn- join-stmt [separator & stmts]
  (let [stmts (map #(if (stmt? %1) %1 (compile-sql %1)) stmts)
        stmts (remove (comp blank? first) stmts)]
    (->> (cons (join separator (map first stmts))
               (apply concat (map rest stmts)))
         (apply vector))))

(defn- stmt [& stmts]
  (apply join-stmt " " stmts))

;; COMPILE CONSTANTS

(defmulti compile-const (fn [node] (class (:form node))))

(defmethod compile-const String [{:keys [form alias]}]
  [(str "?" (if alias (str " AS " (jdbc/as-identifier alias)))) form])

(defmethod compile-const :default [{:keys [form alias]}]
  [(str form (if alias (str " AS " (jdbc/as-identifier alias))))])

;; COMPILE FROM CLAUSE

(defmulti compile-from :op)

(defmethod compile-from :select [node]
  (let [[sql & args] (compile-sql node)]
    (cons (str "(" sql ") AS " (jdbc/as-identifier (:alias node))) args)))

(defmethod compile-from :table [node]
  (compile-sql node))

;; COMPILE SQL

(defmethod compile-sql nil [_]
  nil)

(defmethod compile-sql :column [{:keys [alias schema name table]}]
  [(str (join "." (map jdbc/as-identifier (remove nil? [schema table name])))
        (if alias (str " AS " (jdbc/as-identifier alias))))])

(defmethod compile-sql :constant [node]
  (compile-const node))

(defmethod compile-sql :drop-table [{:keys [cascade if-exists restrict tables]}]
  (let [[sql & args] (apply join-stmt ", " tables)]
    (cons (str "DROP TABLE " (if if-exists "IF EXISTS ") sql
               (if cascade " CASCADE")
               (if restrict " RESTRICT"))
          args)))

(defmethod compile-sql :exprs [{:keys [children]}]
  (let [children (map compile-sql children)]
    (if (empty? children)
      ["*"]
      (cons (join ", " (map first children))
            (apply concat (map rest children))))))

(defmethod compile-sql :fn-call [{:keys [alias args name]}]
  (let [args (map compile-sql args)]
    (cons (str name "(" (join ", " (map first args)) ")"
               (if alias (str " AS " (jdbc/as-identifier alias))))
          (apply concat (map rest args)))))

(defmethod compile-sql :from [{:keys [from]}]
  (let [from (map compile-from from)]
    (cons (str "FROM " (join ", " (map first from)))
          (apply concat (map rest from)))))

(defmethod compile-sql :group-by [{:keys [exprs]}]
  (stmt ["GROUP BY"] exprs))

(defmethod compile-sql :keyword [{:keys [form]}]
  [(jdbc/as-identifier form)])

(defmethod compile-sql :limit [{:keys [count]}]
  [(str "LIMIT " (if (number? count) count "ALL"))])

(defmethod compile-sql :nil [_] ["NULL"])

(defmethod compile-sql :offset [{:keys [start]}]
  [(str "OFFSET " (if (number? start) start 0))])

(defmethod compile-sql :order-by [{:keys [exprs direction nulls using]}]
  (let [[sql & args] (compile-sql exprs)]
    (cons (str "ORDER BY " sql
               (condp = direction
                 :asc " ASC"
                 :desc " DESC"
                 "")
               (condp = nulls
                 :first " NULLS FIRST"
                 :last " NULLS LAST"
                 ""))
          args)))

(defmethod compile-sql :table [{:keys [alias schema name]}]
  [(str (join "." (map jdbc/as-identifier (remove nil? [schema name])))
        (if alias (str " AS " (jdbc/as-identifier alias))))])

(defmethod compile-sql :select [{:keys [exprs from group-by limit offset order-by]}]
  (stmt ["SELECT"] exprs from group-by order-by limit offset))

(defmethod compile-sql :truncate-table [{:keys [cascade tables continue-identity restart-identity restrict]}]
  (stmt ["TRUNCATE TABLE"] (apply join-stmt ", " tables)
        restart-identity continue-identity cascade restrict))

(defmethod compile-sql :truncate-table [{:keys [cascade tables continue-identity restart-identity restrict]}]
  (let [[sql & args] (apply join-stmt ", " tables)]
    (cons (str "TRUNCATE TABLE " sql
               (if restart-identity " RESTART IDENTITY")
               (if continue-identity " CONTINUE IDENTITY")
               (if cascade " CASCADE")
               (if restrict " RESTRICT"))
          args)))
