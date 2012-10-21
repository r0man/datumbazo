(ns database.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [blank? join replace]]))

(defn- append-sql
  "Returns the SQL string for `stmt` with a space at the front."
  [stmt] (if (first stmt) (str " " (first stmt))))

(defn- prepend-sql
  "Returns the SQL string for `stmt` with a space at the end"
  [stmt] (if (first stmt) (str (first stmt) " ")))

(defn- concat-args [& args]
  (apply concat (remove nil? args)))

(defmulti compile-sql :op)

(defn- join-stmt [separator & stmts]
  (let [stmts (map #(if (vector? %1) %1 (compile-sql %1)) stmts)
        stmts (remove (comp blank? first) stmts)]
    (->> (cons (join separator (map first stmts))
               (apply concat (map rest stmts)))
         (apply vector))))

(defn- stmt [& stmts]
  (apply join-stmt " " stmts))

(defmethod compile-sql nil [_]
  nil)

(defmethod compile-sql :cascade [{:keys [cascade]}]
  (if cascade ["CASCADE"]))

(defmethod compile-sql :continue-identity [{:keys [continue-identity]}]
  (if continue-identity ["CONTINUE IDENTITY"]))

(defmethod compile-sql :fn [{:keys [children name]}]
  (let [children (map compile-sql children)]
    (cons (str name "(" (join ", " (map first children)) ")")
          (apply concat (map rest children)))))

(defmethod compile-sql :nil [_]
  ["NULL"])

(defmethod compile-sql :expr-list [{:keys [children]}]
  (let [children (map compile-sql children)]
    (if (empty? children)
      ["*"]
      (cons (join ", " (map first children))
            (apply concat (map rest children))))))

(defmethod compile-sql :if-exists [{:keys [if-exists]}]
  (if if-exists ["IF EXISTS"]))

(defmethod compile-sql :keyword [{:keys [form]}]
  [(jdbc/as-identifier form)])

(defmethod compile-sql :limit [{:keys [count]}]
  [(str "LIMIT " (if (number? count) count "ALL"))])

(defmethod compile-sql :number [{:keys [form]}]
  [(str form)])

(defmethod compile-sql :offset [{:keys [start]}]
  [(str "OFFSET " (if (number? start) start 0))])

(defmethod compile-sql :string [{:keys [form]}]
  ["?" form])

(defmethod compile-sql :table [{:keys [alias schema name]}]
  (assert name)
  [(str (if schema
          (str (jdbc/as-identifier schema) "."))
        (jdbc/as-identifier name)
        (if alias
          (str "AS " (jdbc/as-identifier alias))))])

(defmethod compile-sql :drop-table [{:keys [cascade if-exists restrict children]}]
  (stmt ["DROP TABLE"]
        if-exists
        [(join ", " (map (comp first compile-sql) children))]
        cascade
        restrict))

(defmethod compile-sql :from [{:keys [from]}]
  (if (= :select (:op (first from)))
    (let [subquery (first from)]
      (assert (:as subquery) "Subquery in FROM must have an alias.")
      (join-stmt "" ["FROM ("] (first from) [(str ") AS " (jdbc/as-identifier (:as subquery)))]))
    (stmt ["FROM"] (apply join-stmt ", " from))))

(defmethod compile-sql :order-by [{:keys [expr-list direction nulls using]}]
  (stmt ["ORDER BY"]
        expr-list
        (if direction
          (condp = direction
            :asc ["ASC"]
            :desc ["DESC"]))
        (if nulls
          (condp = nulls
            :first ["NULLS FIRST"]
            :last ["NULLS LAST"]))))

(defmethod compile-sql :restart-identity [{:keys [restart-identity]}]
  (if restart-identity ["RESTART IDENTITY"]))

(defmethod compile-sql :restrict [{:keys [restrict]}]
  (if restrict ["RESTRICT"]))

(defmethod compile-sql :select [{:keys [expr-list from limit offset order-by]}]
  (stmt ["SELECT"]
        expr-list
        from
        order-by
        limit
        offset))

(defmethod compile-sql :truncate-table [{:keys [cascade children continue-identity restart-identity restrict]}]
  (stmt [(str "TRUNCATE TABLE " (join ", " (map (comp first compile-sql) children)))]
        restart-identity
        continue-identity
        cascade
        restrict))
