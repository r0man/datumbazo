(ns database.protocol
  (:require [clojure.java.jdbc :as jdbc]))

(defprotocol Nameable
  (as-keyword [obj]
    "Returns `obj` as a keyword.")
  (as-identifier [obj]
    "Returns `obj` as a database identifier"))

(extend-type clojure.lang.Keyword
  Nameable
  (as-keyword [k]
    (jdbc/as-keyword k))
  (as-identifier [k]
    (jdbc/as-identifier k)))

(extend-type clojure.lang.Symbol
  Nameable
  (as-keyword [s]
    (jdbc/as-keyword s))
  (as-identifier [s]
    (jdbc/as-identifier (keyword s))))

(extend-type java.lang.String
  Nameable
  (as-keyword [s]
    (jdbc/as-keyword s))
  (as-identifier [s]
    (jdbc/as-identifier s)))
