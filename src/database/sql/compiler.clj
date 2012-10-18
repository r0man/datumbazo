(ns database.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace]]))

(defn- concat-args [& args]
  (apply concat (remove nil? args)))

(defmulti compile-sql :op)

(defmethod compile-sql :fn [{:keys [children name]}]
  (let [children (map compile-sql children)]
    (cons (str name "(" (join ", " (map first children)) ")")
          (apply concat (map rest children)))))

(defmethod compile-sql :nil [_]
  ["NULL"])

(defmethod compile-sql :expr-list [{:keys [children]}]
  (let [children (map compile-sql children)]
    (cons (join ", " (map first children))
          (apply concat (map rest children)))))

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

(defmethod compile-sql :table [{:keys [schema name]}]
  (assert name)
  [(str (if schema
          (str (jdbc/as-identifier schema) "."))
        (jdbc/as-identifier name))])

(defmethod compile-sql :drop-table [{:keys [cascade if-exists restrict children]}]
  [(str "DROP TABLE "
        (if if-exists "IF EXISTS ")
        (join ", " (map (comp first compile-sql) children))
        (if cascade " CASCADE")
        (if restrict " RESTRICT"))])

(defmethod compile-sql :select [{:keys [expressions from-item limit offset]}]
  (let [expressions (map compile-sql expressions)
        from-item (map compile-sql from-item)
        limit (if limit (compile-sql limit))
        offset (if offset (compile-sql offset))]
    (concat [(str "SELECT " (if (empty? expressions)
                              "*" (join ", " (map first expressions)))
                  (if-not (empty? from-item)
                    (str " FROM " (join ", " (map first from-item))))
                  (if limit (str " " (first limit)))
                  (if offset (str " " (first offset))))]
            (concat-args (apply concat (map rest expressions))
                         (if limit (rest limit))
                         (if offset (rest offset))))))

(defmethod compile-sql :truncate-table [{:keys [cascade children continue-identity restart-identity restrict]}]
  [(str "TRUNCATE TABLE "
        (join ", " (map (comp first compile-sql) children))
        (if restart-identity " RESTART IDENTITY")
        (if continue-identity " CONTINUE IDENTITY")
        (if cascade " CASCADE")
        (if restrict " RESTRICT"))])
