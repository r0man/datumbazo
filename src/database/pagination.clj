(ns database.pagination
  (:use database.util
        korma.core))

(def ^:dynamic *page* 1)
(def ^:dynamic *per-page* 15)

(defn parse-page
  "Parse `string` as the page number or return the default *page*."
  [string] (or (parse-integer string :junk-allowed true) *page*))

(defn parse-per-page
  "Parse `string` as the per page number or return the default *per-page*."
  [string] (or (parse-integer string :junk-allowed true) *per-page*))

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
  (let [page (parse-page page) per-page (parse-per-page per-page)]
    (-> (offset query (* (dec page) per-page))
        (limit per-page)
        (exec))))

(defn paginate*
  [query & {:keys [page per-page]}]
  (let [page (parse-page page)
        per-page (parse-per-page per-page)
        result (result-query query page per-page)]
    (if (instance? clojure.lang.IMeta result)
      (with-meta result
        {:page page :per-page per-page :total (count-query query)})
      result)))

(defmacro paginate
  "Paginate the `query` with `page` and `per-page`."
  [query & {:keys [count page per-page]}]
  `(paginate*
    (query-only ~query)
    :page ~page :per-page ~per-page))

(defn wrap-pagination
  "Ring middleware that parses the pagination parameters from
  Ring's :params map and binds the *page* and *per-page* vars."
  [handler & [page per-page]]
  (let [page (or page :page)
        per-page (or per-page :per-page)]
    (fn [{:keys [params] :as request}]
      (binding [*page* (parse-page (get params page))
                *per-page* (parse-per-page (get params per-page))]
        (handler request)))))