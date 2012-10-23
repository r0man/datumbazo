(ns datumbazo.relation
  (:require [clojure.java.jdbc :as jdbc]
            [sqlingvo.core :refer [sql]]
            [sqlingvo.util :refer [parse-expr]]))

(defn- count-query
  "Transform `query` into a count(*) query."
  [query]
  (let [exprs [(parse-expr '(count *))]]
    (assoc query :exprs {:op :exprs :children exprs})))

(deftype Relation [stmt]

  clojure.lang.IPersistentCollection
  (count [this]
    (jdbc/with-query-results results
      (sql (count-query stmt))
      (:?column? (first results))))

  (equiv [this other]
    (and (isa? (class other) Relation)
         (= (.-stmt other) stmt)))

  clojure.lang.Seqable
  (seq [this]
    (jdbc/with-query-results results
      (sql stmt)
      (doall results)))

  Object
  (toString [this]
    (str (sql stmt))))
