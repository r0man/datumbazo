(ns database.pagination
  (:require [korma.core :as k])
  (:use [korma.core :exclude (join table offset)]
        database.util))

(def ^:dynamic *per-page* 15)

(defn- assert-page
  "Ensure that page is greater than zero."
  [page]
  (if-not (pos? page)
    (throw (IllegalArgumentException. "The \"page\" parameter must be greater than 0."))))

(defn- assert-per-page
  "Ensure that per-page is not negative."
  [per-page]
  (if (neg? per-page)
    (throw (IllegalArgumentException. "The \"per-page\" parameter can't be negative."))))

(defn- offset
  "Calculate the OFFSET clause from page and per-page."
  [page per-page]
  (assert-page page)
  (assert-per-page per-page)
  (* (dec page) per-page))

(defmacro with-params [page per-page & body]
  (let [page# page per-page# per-page]
    `(let [~page# (or (parse-int ~page#) 1)
           ~per-page# (or (parse-int ~per-page#) *per-page*)]
       (assert-page ~page#)
       (assert-per-page ~per-page#)
       ~@body)))

(defn- remove-transformations [query]
  (if (and (map? query)
           (:ent query)
           (map? (:ent query))
           (:transforms (:ent query)))
    (assoc-in query [:ent :transforms] [])
    query))

(defn- count-query
  "Transform `query` into a count query."
  [query]
  (-> (assoc query :fields [:korma.core/*])
      (dissoc :order)
      (remove-transformations)
      (aggregate (count :*) :count)
      exec first :count))

(defn- result-query
  "Transform `query` into a result query."
  [query page per-page]
  (with-params page per-page
    (-> (k/offset query (offset page per-page))
        (k/limit per-page)
        (exec))))

(defn paginate*
  [query & {:keys [page per-page]}]
  (with-params page per-page
    (let [result (result-query query page per-page)]
      (if (instance? clojure.lang.IMeta result)
        (with-meta result
          {:page page :per-page per-page :total (count-query query)})
        result))))

(defmacro paginate
  "Paginate the `query` with `page` and `per-page`."
  [query & {:keys [count page per-page]}]
  `(paginate*
    (with-redefs [exec identity] (doall ~query))
    :page ~page :per-page ~per-page))
