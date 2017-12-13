(ns datumbazo.select-batch
  (:require [clojure.spec.alpha :as s]
            [datumbazo.record :refer [select-column-expr]]
            [datumbazo.util :refer [table-keyword]]
            [sqlingvo.core :as sql]))

(defn- index-rows
  "Convert the seq of `rows` into a seq of vectors."
  [table rows & [{:keys [columns types]}]]
  (let [columns (:columns table)]
    (with-meta
      (for [[index row] (map-indexed vector rows)]
        (->> (for [column columns
                   :let [type (get-in table [:column column :type])]]
               (if type
                 `(cast ~(get row column)
                        ~(case type
                           :bigserial :bigint
                           :serial :integer
                           type))
                 (get row column)))
             (concat [index])
             (vec)))
      {:columns columns :types types})))

(s/fdef index-rows
  :args (s/cat :table :sqlingvo/table
               :rows (s/coll-of map?)
               :opts (s/? (s/nilable map?))))

(defn- source-values
  [table rows & [{:keys [types] :as opts}]]
  (let [values (index-rows table rows opts)
        column-aliases (concat [:index] (:columns table))]
    (sql/as (sql/values values) (table-keyword table) column-aliases)))

(s/fdef source-values
  :args (s/cat :alias :sqlingvo/table
               :rows (s/coll-of map?)
               :opts (s/? (s/nilable map?))))

(defn- source-table
  [target]
  (-> (assoc target :name :source)
      (dissoc :schema)))

(defn- target-table
  [x]
  (cond
    (keyword? x)
    (sql/table x
      (sql/column :id :integer :primary-key? true))
    (and (map? x)
         (= (:op x) :table))
    x
    :else (throw (ex-info (str "Invalid table: " (pr-str x))))))

(defn- join-condition
  "Returns the JOIN condition for the batch select."
  [source target]
  (sql/join (table-keyword target)
            `(on (or ~@(for [column (filter :primary-key? (vals (:column target)))]
                         `(= ~(keyword (str (-> source :name name) "."
                                            (-> column :name name)))
                             ~(keyword (str (-> target :name name) "."
                                            (-> column :name name)))))))
            :type :left))

(s/fdef join-condition
  :args (s/cat :source :sqlingvo/table :target :sqlingvo/table))

(defn- order-by-column
  "Returns the ORDER BY column for the `source` table."
  [source]
  (keyword (str (-> source :name name) ".index")))

(s/fdef order-by-column
  :args (s/cat :source :sqlingvo/table))

(defn- select-all-columns
  "Returns the `*` SELECT expression for `table`"
  [db table]
  [(keyword (str (-> table :name name) ".*"))])

(s/fdef select-all-columns
  :args (s/cat :db sql/db? :table :sqlingvo/table))

(defn- select-table-columns
  "Returns the column SELECT expression for `table`"
  [db table]
  (map #(select-column-expr db table %)
       (-> table :column vals)))

(s/fdef select-table-columns
  :args (s/cat :db sql/db? :table :sqlingvo/table))

(defn select-batch
  "Select the batch of `rows` from `table` in `db`."
  [db table rows & [{:keys [where]}]]
  (when-not (empty? rows)
    (let [target (target-table table)
          source (source-table target)]
      (->> @(sql/select db (if (keyword? table)
                             (select-all-columns db target)
                             (select-table-columns db target))
              (sql/from (source-values source rows))
              (join-condition source target)
              (when where (sql/where where :and))
              (sql/order-by (order-by-column source)))
           (map (fn [row]
                  (let [vals (remove nil? (map row (:columns target)))]
                    (when-not (empty? vals)
                      row))))))))

(s/fdef select-batch
  :args (s/cat :db sql/db?
               :table (s/or :keyword keyword? :table :sqlingvo/table)
               :rows (s/coll-of map?)
               :opts (s/? (s/nilable map?))))
