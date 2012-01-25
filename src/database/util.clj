(ns database.util
  (:require [clojure.java.jdbc :as jdbc]))

(defn make-sql-array
  "Make a java.sql.Array of `type` from `coll`."
  [type coll]
  (let [connection (jdbc/find-connection)]
    (assert connection "No database connection.")
    (.createArrayOf connection type (object-array coll))))

(defn make-text-array
  "Make a text array from `coll`."
  [coll] (make-sql-array "text" (map str coll)))

(defn parse-int [s]
  (try (Integer/parseInt (str s))
       (catch NumberFormatException _ nil)))
