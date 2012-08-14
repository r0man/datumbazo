(ns database.protocol
  (:require [clojure.java.jdbc :as jdbc]))

(defprotocol Nameable
  (as-identifier [obj]
    "Returns the database identifier of `obj`."))

(extend-type clojure.lang.Keyword
  Nameable
  (as-identifier [k]
    (jdbc/as-identifier k)))

(extend-type clojure.lang.Symbol
  Nameable
  (as-identifier [s]
    (jdbc/as-identifier (keyword s))))

(extend-type java.lang.String
  Nameable
  (as-identifier [s]
    (jdbc/as-identifier s)))
