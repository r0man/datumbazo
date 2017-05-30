(ns datumbazo.pagination
  (:require [no.en.core :refer [parse-integer]]
            [sqlingvo.core :as sql]))

(def ^:dynamic *page*
  "The current page number."
  nil)

(def ^:dynamic *per-page*
  "The default number of pages."
  25)

(defn strip-query
  "Remove LIMIT and OFFSET clauses from `query`."
  [query]
  (sql/compose
   query
   (sql/limit nil)
   (sql/offset nil)))

(defn- results
  "Returns the total number of results for `query`."
  [db query]
  (->> @(sql/with db [:rows (strip-query query)]
          (sql/select db ['(count :*)]
            (sql/from :rows)))
       first :count))

(defn- page
  "Returns :page of `params` or 1."
  [params]
  (or (:page params) 1))

(defn- per-page
  "Returns :per-page of `params` or `*per-page*`."
  [params]
  (or (:per-page params) *per-page*))

(defn- pages
  "Returns the total number of pages for `results` and `per-page`."
  [results per-page]
  (inc (int (/ (dec results) per-page))))

(defn query
  "Returns the query for `target`."
  [target]
  (cond
    (instance? sqlingvo.expr.Stmt target)
    target
    (sequential? target)
    (-> target meta :datumbazo/stmt)
    :else
    (throw (ex-info (str "Can't infer query." {:target target})))))

(defn page-info
  "Returns the page info for `query` and `params`."
  [db target & [params]]
  (let [query (query target)
        results (results db query)]
    {:page (page params)
     :pages (pages results (per-page params))
     :per-page (per-page params)
     :results results}))

(defn paginate
  "Add LIMIT and OFFSET clauses to `query` calculated from `page` and
  `per-page.`"
  [page & [limit per-page]]
  (let [page (parse-integer (or page *page*))
        per-page (parse-integer (or limit per-page *per-page*))]
    (fn [stmt]
      (if page
        ((sql/chain-state
          [(sql/limit per-page)
           (sql/offset (* (dec page) (or per-page *per-page*)))])
         stmt)
        [nil stmt]))))
