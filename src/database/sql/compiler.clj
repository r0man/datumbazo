(ns database.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace]]))

(defn- append-sql
  "Returns the SQL string for `stmt` with a space at the front."
  [stmt] (if (first stmt) (str " " (first stmt))))

(defn- prepend-sql
  "Returns the SQL string for `stmt` with a space at the end"
  [stmt] (if (first stmt) (str (first stmt) " ")))

(defn- concat-args [& args]
  (apply concat (remove nil? args)))

(defmulti compile-sql :op)

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
    (cons (join ", " (map first children))
          (apply concat (map rest children)))))

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

(defmethod compile-sql :table [{:keys [schema name]}]
  (assert name)
  [(str (if schema
          (str (jdbc/as-identifier schema) "."))
        (jdbc/as-identifier name))])

(defmethod compile-sql :drop-table [{:keys [cascade if-exists restrict children]}]
  (let [cascade (compile-sql cascade)
        if-exists (compile-sql if-exists)
        restrict (compile-sql restrict)]
    [(str "DROP TABLE " (prepend-sql if-exists)
          (join ", " (map (comp first compile-sql) children))
          (append-sql cascade)
          (append-sql restrict))]))

(defmethod compile-sql :restart-identity [{:keys [restart-identity]}]
  (if restart-identity ["RESTART IDENTITY"]))

(defmethod compile-sql :restrict [{:keys [restrict]}]
  (if restrict ["RESTRICT"]))

(defmethod compile-sql :select [{:keys [expressions from limit offset]}]
  (let [expressions (map compile-sql expressions)
        from (map compile-sql from)
        limit (if limit (compile-sql limit))
        offset (if offset (compile-sql offset))]
    (cons (str "SELECT " (if (empty? expressions)
                           "*" (join ", " (map first expressions)))
               (if-not (empty? from)
                 (str " FROM " (join ", " (map first from))))
               (append-sql limit)
               (append-sql offset))
          (concat-args (apply concat (map rest expressions))
                       (if limit (rest limit))
                       (if offset (rest offset))))))

(defmethod compile-sql :truncate-table [{:keys [cascade children continue-identity restart-identity restrict]}]
  (let [cascade (compile-sql cascade)
        restrict (compile-sql restrict)
        continue-identity (compile-sql continue-identity)
        restart-identity (compile-sql restart-identity)]
    [(str "TRUNCATE TABLE " (join ", " (map (comp first compile-sql) children))
          (append-sql restart-identity)
          (append-sql continue-identity)
          (append-sql cascade)
          (append-sql restrict))]))
