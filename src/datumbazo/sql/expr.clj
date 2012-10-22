(ns datumbazo.sql.expr
  (:require [datumbazo.util :as u]))

(defmulti parse-expr class)

(defn parse-fn-expr [expr]
  {:op :fn-call
   :name (first expr)
   :args (map parse-expr (rest expr))})

(defmethod parse-expr nil [expr]
  {:op :nil})

(defmethod parse-expr clojure.lang.Cons [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.PersistentList [expr]
  (parse-fn-expr expr))

(defmethod parse-expr clojure.lang.IPersistentMap [expr]
  expr)

(defmethod parse-expr clojure.lang.Keyword [expr]
  (u/parse-column expr))

(defmethod parse-expr clojure.core$_STAR_ [expr]
  {:op :constant :form '*})

(defmethod parse-expr :default [expr]
  {:op :constant :form expr})

(defn parse-exprs [exprs]
  {:op :exprs :children (map parse-expr exprs)})