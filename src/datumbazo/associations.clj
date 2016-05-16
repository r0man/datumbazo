(ns datumbazo.associations
  (:require [clojure.string :as str]
            [datumbazo.util :as util]
            [inflections.core :as infl]
            [schema.core :as s]
            [sqlingvo.core :as sql]))

(defprotocol IAssociation
  (fetch [association record]
    "Fetch all records in `association` of `record`."))

(s/defrecord BelongsTo
    [class-name :- s/Str
     foreign-key :- s/Keyword
     name :- s/Keyword
     primary-key :- s/Keyword
     table :- s/Keyword])

(s/defrecord HasMany
    [batch-size :- s/Int
     class-name :- s/Str
     dependent :- s/Bool
     foreign-key :- s/Keyword
     name :- s/Keyword
     primary-key :- s/Keyword
     table :- s/Keyword])

(s/defn ^:private class-name :- s/Str
  "Return the class name for `association`."
  [association :- s/Keyword]
  (->> [(infl/plural (name association))
        (util/class-symbol {:name association})]
       (concat (butlast (str/split (str *ns*) #"\.")))
       (str/join ".")))

(defn belongs-to
  "Add a belongs to association to a table."
  [association & {:keys [foreign-key primary-key schema]}]
  (fn [table]
    (let [node (map->BelongsTo
                {:class-name (class-name association)
                 :foreign-key (or foreign-key (infl/foreign-key association "-"))
                 :name association
                 :primary-key (or primary-key :id)
                 :schema schema
                 :table (keyword (infl/plural association))})]
      [node (assoc-in table [:associations association] node)])))

(defn has-many
  "Add a has many association to a table."
  [association & {:keys [batch-size dependent foreign-key primary-key schema]}]
  (fn [table]
    (let [node (map->HasMany
                {:batch-size (or batch-size 100)
                 :class-name (class-name association)
                 :dependent dependent
                 :foreign-key (or foreign-key (infl/foreign-key (:name table) "-"))
                 :name association
                 :primary-key (or primary-key :id)
                 :schema schema
                 :table (keyword association)})]
      [node (assoc-in table [:associations association] node)])))

(extend-type BelongsTo
  IAssociation
  (fetch [association record]
    (let [class (util/resolve-class (:class-name association))
          columns (util/columns-by-class class)]
      (->> @(sql/select (.db record) (map #(util/column-keyword % true) columns)
                        (sql/from
                         (util/table-keyword
                          {:schema (:schema association)
                           :name (:table association)}))
                        (sql/where `(= ~(:primary-key association)
                                       ~(get record (:foreign-key association)))))
           (util/make-instances (.db record) class)
           (first)))))

(extend-type HasMany
  IAssociation
  (fetch [association record]
    (let [class (util/resolve-class (:class-name association))
          table (util/table-by-class class)
          columns (util/columns-by-class class)]
      (->> (util/fetch-batch
            (sql/select (.db record) (map #(util/column-keyword % true) columns)
                        (sql/from (util/table-keyword table))
                        (sql/where `(= ~(keyword (str (name (:name association)) "."
                                                      (name (:foreign-key association))))
                                       ~(:id record))))
            {:size (:batch-size association)})
           (util/make-instances (.db record) class)))))
