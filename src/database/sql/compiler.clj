(ns database.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace]]))

(defmulti compile-sql :op)

(defmethod compile-sql :fn [{:keys [children form]}]
  (let [child-exprs (map compile-sql children)
        sql (join ", " (map first child-exprs))]
    (concat [(str form "(" sql ")")]
            (apply concat (map rest child-exprs)))))

(defmethod compile-sql :nil [_]
  ["NULL"])

(defmethod compile-sql :keyword [{:keys [form]}]
  [(jdbc/as-identifier form)])

(defmethod compile-sql :number [{:keys [form]}]
  [(str form)])

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

(defmethod compile-sql :select [{:keys [expressions from-item]}]
  (let [expressions (map compile-sql expressions)
        from-item (map compile-sql from-item)]
    (concat [(str "SELECT " (if (empty? expressions)
                              "*" (join ", " (map first expressions)))
                  (if-not (empty? from-item)
                    (str " FROM " (join ", " (map first from-item)))))]
            (apply concat (map rest expressions)))))

(defmethod compile-sql :truncate-table [{:keys [cascade children continue-identity restart-identity restrict]}]
  [(str "TRUNCATE TABLE "
        (join ", " (map (comp first compile-sql) children))
        (if restart-identity " RESTART IDENTITY")
        (if continue-identity " CONTINUE IDENTITY")
        (if cascade " CASCADE")
        (if restrict " RESTRICT"))])