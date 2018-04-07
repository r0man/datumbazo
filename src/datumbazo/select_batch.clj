(ns datumbazo.select-batch
  (:require [clojure.spec.alpha :as s]
            [datumbazo.record :refer [select-column-expr]]
            [datumbazo.util :refer [table-keyword]]
            [sqlingvo.core :as sql]))

(defn- primary-keys [table]
  (filter :primary-key? (vals (:column table))))

(defn- join-columns
  [target & [{:keys [join] :as opts}]]
  (or (not-empty join)
      (->> (for [column (primary-keys target)]
             [(:name column) (:type column)])
           (into {} )
           (not-empty))
      {:id :integer}))

(defn- index-rows
  "Convert the seq of `rows` into a seq of vectors."
  [table rows & [opts]]
  (for [[index row] (map-indexed vector rows)]
    (->> (for [[column-name column-type] (join-columns table opts)]
           `(cast ~(get row column-name)
                  ~(case column-type
                     :bigserial :bigint
                     :serial :integer
                     column-type)))
         (concat [index])
         (vec))))

(s/fdef index-rows
  :args (s/cat :table :sqlingvo/table
               :rows (s/coll-of map?)
               :opts (s/? (s/nilable map?))))

(defn- source-values
  [table rows & [opts]]
  (let [values (index-rows table rows opts)
        column-aliases (concat [:index] (keys (join-columns table opts)))]
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
  [source target & [opts]]
  (sql/join
   (table-keyword target)
   `(on (or ~@(for [[column-name column-type] (join-columns target opts)]
                `(= ~(keyword (str (-> source :name name) "."
                                   (-> column-name name)))
                    ~(keyword (str (-> target :name name) "."
                                   (-> column-name name)))))))
   :type :left))

(s/fdef join-condition
  :args (s/cat :source :sqlingvo/table
               :target :sqlingvo/table
               :opts (s/? (s/nilable map?))))

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
  [db table & [{:keys [except]}]]
  (let [columns (-> table :column vals)
        except (set except)]
    (cond->> columns
      (seq except) (remove #(contains? except (:name %)))
      true (map #(select-column-expr db table %)))))

(s/fdef select-table-columns
  :args (s/cat :db sql/db?
               :table :sqlingvo/table
               :opts (s/* (s/nilable map?))))

(defn- select-columns
  [db table target & [opts]]
  (if (keyword? table)
    (select-all-columns db target)
    (select-table-columns db target opts)))

(defn select-batch-sql
  "Select the batch of `rows` from `table` in `db`."
  [db table rows & [{:keys [except join where] :as opts}]]
  (when-not (empty? rows)
    (let [target (target-table table)
          source (source-table target)]
      (sql/select db (select-columns db table target opts)
        (sql/from (source-values source rows opts))
        (join-condition source target opts)
        (when where (sql/where where :and))
        (sql/order-by (order-by-column source))))))

(defn select-batch
  "Select the batch of `rows` from `table` in `db`."
  [db table rows & [{:keys [except join where] :as opts}]]
  (when-not (empty? rows)
    (let [target (target-table table)
          columns (keys (join-columns target opts))]
      (->> @(select-batch-sql db table rows opts)
           (map (fn [row]
                  (let [vals (remove nil? (map row columns))]
                    (when-not (empty? vals)
                      row))))))))

(s/fdef select-batch
  :args (s/cat :db sql/db?
               :table (s/or :keyword keyword? :table :sqlingvo/table)
               :rows (s/coll-of map?)
               :opts (s/? (s/nilable map?))))
