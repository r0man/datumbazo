(ns database.util
  (:import [com.mchange.v2.c3p0 C3P0ProxyConnection])
  (:require [clojure.java.jdbc :as jdbc])
  (:use korma.db))

;; (defn make-sql-array
;;   "Make a java.sql.Array of `type` from `coll`."
;;   [type coll]
;;   (let [connection (.getConnection (:datasource (get-connection @_default)))]
;;     (assert connection "No database connection.")
;;     (.createArrayOf connection type (object-array coll))))

;; (defn make-text-array
;;   "Make a text array from `coll`."
;;   [coll] (make-sql-array "text" (map str coll)))

;; (defn sql-array-seq
;;   "Make a text array from `coll`."
;;   [array] (map :value (resultset-seq (.getResultSet array))))

(defn parse-int [s]
  (try (Integer/parseInt (str s))
       (catch NumberFormatException _ nil)))
