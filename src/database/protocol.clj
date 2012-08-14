(ns database.protocol
  (:require [clojure.java.jdbc :as jdbc]))

(defprotocol IIdentifier
  (as-identifier [obj]
    "Returns the database identifier of `obj`."))

(extend-type clojure.lang.Keyword
  IIdentifier
  (as-identifier [k]
    (jdbc/as-identifier k)))

(extend-type clojure.lang.Symbol
  IIdentifier
  (as-identifier [s]
    (jdbc/as-identifier (keyword s))))

(extend-type java.lang.String
  IIdentifier
  (as-identifier [s]
    (jdbc/as-identifier s)))
