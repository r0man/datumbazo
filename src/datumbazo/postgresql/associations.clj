(ns datumbazo.postgresql.associations
  (:require [clojure.core :as core]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [datumbazo.util :as util :refer [column-keyword table-keyword]]
            [inflections.core :as infl]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]))

(s/def ::batch
  (s/coll-of map?))

(s/def ::foreign-key keyword?)
(s/def ::join-table keyword?)
(s/def ::limit (s/nilable nat-int?))
(s/def ::offset (s/nilable nat-int?))
(s/def ::primary-key keyword?)
(s/def ::source keyword?)
(s/def ::target keyword?)
(s/def ::order-by any?)

(s/def ::table (s/or :keyword keyword? :table :sqlingvo/table))

(s/def ::belongs-to-opts
  (s/keys :req-un [::source ::target]
          :opt-un [::foreign-key ::primary-key]))

(s/def ::has-one-opts
  (s/keys :req-un [::source ::target]
          :opt-un [::foreign-key ::primary-key]))

(s/def ::has-many-opts
  (s/keys :opt-un [::limit ::offset ::order-by]))

(s/def ::has-and-belongs-to-many-opts
  (s/keys :opt-un [::join-table ::limit ::offset ::order-by]))

(s/def ::foreign-key-opts
  (s/keys :opt-un [:sqlingvo.table/schema
                   :sqlingvo.table/name]))

(s/def ::primary-key-opts
  (s/keys :opt-un [:sqlingvo.table/schema
                   :sqlingvo.table/name]))

(defn- extract-many [primary-key row]
  (when-let [pk-vals (remove nil? (:array_paginate row))]
    (with-meta (mapv #(hash-map (:name primary-key) %) pk-vals)
      (merge (meta row) row))))

(defn- extract-one [primary-key row]
  (when (get row (:name primary-key))
    row))

(defn- foreign-key
  "Returns the foreign key for `table`."
  [source target & [opts]]
  (let [source (expr/parse-table source)
        target (expr/parse-table target)]
    {:op :column
     :schema (:schema target)
     :table (:table target)
     :name (->> [(infl/singular (:name source))
                 (or (:prefix opts) :id)]
                (remove nil?)
                (map core/name)
                (str/join "-")
                (keyword))}))

(s/fdef foreign-key
  :args (s/cat :source ::table :target ::table :opts (s/? ::foreign-key-opts)))

(defn- primary-key
  "Returns the primary key for `table`."
  [table & [opts]]
  (let [table (expr/parse-table table)
        schema (or (:schema table) (:schema opts))
        name (or (:name table) (:name opts))]
    (cond-> {:op :column :name :id}
      schema (assoc :schema schema)
      name (assoc :table name))))

(s/fdef primary-key
  :args (s/cat :table ::table :opts (s/? ::primary-key-opts)))

(defn- array-paginate-start
  "Returns the start position in the pagination array."
  [{:keys [limit offset]}]
  (cond
    (number? offset)
    (inc offset)
    (number? limit)
    1))

(defn- array-paginate-end
  "Returns the end  position in the pagination array."
  [{:keys [limit offset]}]
  (cond
    (number? limit)
    (+ (or offset 0) limit)))

(defn- array-paginate
  "Returns the array pagination expr."
  [primary-key & [{:keys [limit offset order-by] :as opts}]]
  (let [order-by (if order-by [order-by])]
    (sql/as
     (if (or (:limit opts) (:offset opts))
       `(array_subvec
         (array_agg
          ~(column-keyword primary-key)
          ~@order-by)
         ~(array-paginate-start opts)
         ~(array-paginate-end opts))
       `(array_agg
         ~(column-keyword primary-key)
         ~@order-by))
     :array_paginate)))

(defn- source-batch-values
  [db batch source & [opts]]
  (let [primary-key (primary-key source)]
    (sql/as
     (sql/values
      (for [[index row] (map-indexed vector batch)]
        ;; Cast primary-key, because it could be NULL
        [`(cast ~(get row (:name primary-key))
                ~(or (:source-type opts) :int))
         index]))
     :source-batch [(:name primary-key) :index])))

(s/fdef source-batch-values
  :args (s/cat :db sql/db?
               :batch (s/coll-of map?)
               :source :sqlingvo/table))

;; Belongs to

(defn belongs-to [db batch source target & [opts]]
  (let [source (expr/parse-table source)
        target (expr/parse-table target)
        source-pk (primary-key source)
        source-fk (foreign-key target source)
        target-pk (primary-key target)]
    (->> @(sql/select db [(sql/as (column-keyword source-fk) (:name target-pk))]
            (sql/from (source-batch-values db batch source))
            (sql/join (table-keyword source)
                      `(on (= ~(keyword (str "source-batch." (-> source-pk :name name)))
                              ~(column-keyword source-pk)))
                      :type :left)
            (sql/order-by :source-batch.index))
         (mapv #(extract-one target-pk %)))))

(s/fdef belongs-to
  :args (s/cat :db sql/db?
               :batch ::batch
               :source keyword?
               :target keyword?
               :opts (s/? ::belongs-to-opts)))

;; Has Many

(defn has-many [db batch source target & [opts]]
  (let [source (expr/parse-table source)
        target (expr/parse-table target)
        source-pk (primary-key source)
        source-fk (foreign-key source target)
        target-pk (primary-key target)]
    (->> @(sql/select db [(keyword (str "source." (-> source-pk :name name)))
                          `(count ~(column-keyword target-pk))
                          (array-paginate target-pk opts)]
            (sql/from (source-batch-values db batch source))
            (sql/join (sql/as (table-keyword source) :source)
                      `(on (= ~(keyword (str "source-batch." (-> source-pk :name name)))
                              ~(keyword (str "source." (-> source-pk :name name)))))
                      :type :left)
            (sql/join (sql/as
                       (sql/select db [:*]
                         (sql/from target)
                         (some-> opts :where))
                       (-> target :name keyword))
                      `(on (= ~(keyword (str "source." (-> source-pk :name name)))
                              ~(column-keyword (assoc source-fk :table (-> target :name keyword)))))
                      :type :left)
            (sql/where `(and (in ~(keyword (str "source." (-> source-pk :name name)))
                                 ~(map (:name source-pk) batch)))
                       :and)
            (sql/group-by :source-batch.index (keyword (str "source." (-> source-pk :name name))))
            (sql/order-by :source-batch.index))
         (mapv #(extract-many target-pk %)))))

(s/fdef has-many
  :args (s/cat :db sql/db?
               :batch ::batch
               :source keyword?
               :target keyword?
               :opts (s/? ::has-many-opts)))

;; Has one

(defn has-one [db batch source target & [opts]]
  (let [source (expr/parse-table source)
        target (expr/parse-table target)
        source-pk (primary-key source)
        target-pk (primary-key target)
        target-fk (foreign-key source target)]
    (->> @(sql/select db [(column-keyword target-pk)]
            (sql/from (source-batch-values db batch source))
            (sql/join (table-keyword source)
                      `(on (= ~(keyword (str "source-batch." (-> source-pk :name name)))
                              ~(column-keyword source-pk)))
                      :type :left)
            (sql/join (table-keyword target)
                      `(on (= ~(column-keyword source-pk)
                              ~(column-keyword target-fk)))
                      :type :left)
            (sql/order-by :source-batch.index))
         (mapv #(extract-one target-pk %)))))

(s/fdef has-one
  :args (s/cat :db sql/db?
               :batch ::batch
               :source keyword?
               :target keyword?
               :opts (s/? ::has-one-opts)))

;; Has and belongs to many

(defn- habtm-association-schema
  "Returns the association schema from :association-schema in `opts`
  or :schema in `source` or `target`."
  [source target & [opts]]
  (or (:association-schema opts)
      (:schema target)
      (:schema source)))

(defn- habtm-association-name
  "Returns the has and belongs to many join table name for `source`,
  `target` and `opts`."
  [source target & [opts]]
  (->> [(-> source :name name) (-> target :name name)]
       sort (str/join "-") keyword))

(defn- habtm-association
  "Returns the has and belongs to many association for `source`,
  `target` and `opts`."
  [source target & [opts]]
  (let [schema (habtm-association-schema source target opts)]
    (cond-> {:op :table :name (habtm-association-name source target opts)}
      schema (assoc :schema schema))))

(defn- habtm-join-table
  "Returns the has and belongs to many join table for `source`, `target`
  and `opts`."
  [source target & [opts]]
  (or (some-> opts :join-table expr/parse-table)
      (habtm-association source target opts)))

(defn has-and-belongs-to-many [db batch source target & [opts]]
  (let [source (expr/parse-table source)
        target (expr/parse-table target)
        join-table (habtm-join-table source target opts)
        source-pk (primary-key source)
        source-fk (foreign-key source target)
        join-source-fk (foreign-key source join-table)
        join-target-fk (foreign-key target join-table)
        target-pk (primary-key target)]
    (->> @(sql/select db [(column-keyword source-pk)
                          `(count ~(column-keyword target-pk))
                          (array-paginate target-pk opts)]
            (sql/from (source-batch-values db batch source))
            (sql/join (table-keyword source)
                      `(on (= ~(keyword (str "source-batch." (-> source-pk :name name)))
                              ~(column-keyword source-pk)))
                      :type :left)
            (sql/join join-table
                      `(on (= ~(column-keyword source-pk)
                              ~(column-keyword join-source-fk)))
                      :type :left)
            (sql/join (sql/as
                       (sql/select db [:*]
                                   (sql/from target)
                                   (some-> opts :where))
                       (-> target :name keyword))
                      `(on (= ~(column-keyword (assoc join-target-fk :table (:name join-table)))
                              ~(column-keyword target-pk)))
                      :type :left)
            (sql/where `(and (in ~(column-keyword source-pk)
                                 ~(map (:name source-pk) batch))))
            (sql/group-by :source-batch.index (column-keyword source-pk))
            (sql/order-by :source-batch.index))
         (mapv #(extract-many target-pk %)))))

(s/fdef has-and-belongs-to-many
  :args (s/cat :db sql/db?
               :batch ::batch
               :source keyword?
               :target keyword?
               :opts (s/? ::has-and-belongs-to-many-opts)))
