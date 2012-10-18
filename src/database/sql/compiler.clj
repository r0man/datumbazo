(ns database.sql.compiler
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :refer [join replace]]))

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
               (if limit (str " " (first limit)))
               (if offset (str " " (first offset))))
          (concat-args (apply concat (map rest expressions))
                       (if limit (rest limit))
                       (if offset (rest offset))))))

(defmethod compile-sql :truncate-table [{:keys [cascade children continue-identity restart-identity restrict]}]
  (let [cascade (compile-sql cascade)
        restrict (compile-sql restrict)
        continue-identity (compile-sql continue-identity)
        restart-identity (compile-sql restart-identity)]
    [(str "TRUNCATE TABLE "
          (join ", " (map (comp first compile-sql) children))
          (if restart-identity (str " " (first restart-identity)))
          (if continue-identity (str " " (first continue-identity)))
          (if cascade (str " " (first cascade)))
          (if restrict (str " " (first restrict))))]))
