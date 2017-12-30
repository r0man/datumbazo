(ns datumbazo.information-schema
  (:require [clojure.spec.alpha :as s]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]
            [sqlingvo.util :as util]))

(s/def ::column-name string?)
(s/def ::identifier keyword?)
(s/def ::table-name string?)
(s/def ::table-schema string?)

(s/def ::column
  (s/keys :req-un [::column-name
                   ::table-name
                   ::table-schema]))

(s/def ::columns
  (s/map-of string? ::column))

(s/def ::column-names
  (s/coll-of ::column-name))

(s/def ::table
  (s/keys :req-un [::column-names
                   ::columns
                   ::table-name
                   ::table-schema]))

(defn column-name
  "Returns the column name of `identifier`."
  [db identifier]
  (->> (expr/parse-column identifier) :name (util/sql-name db)))

(s/fdef column-name
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret string?)

(defn table-name
  "Returns the table name of `identifier`."
  [db k]
  (->> (expr/parse-table k) :name (util/sql-name db)))

(s/fdef table-name
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret string?)

(defn table-catalog
  "Returns the table catalog of `db`."
  [db]
  (:name db))

(defn table-schema
  "Returns the table schema of `identifier`."
  [db k]
  (or (->> (expr/parse-table k) :schema (util/sql-name db))
      ;; TODO: User db name for mysql
      "public"))

(s/fdef table-schema
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret string?)

(defn- schema-db
  "Returns `db` using a Clojure naming strategy."
  [db]
  (->> {:sql-name util/sql-name-underscore
        :sql-keyword util/sql-keyword-hyphenate}
       (merge db)))

(defn- zip-column-name
  [columns]
  (zipmap (map :column-name columns) columns))

(defn primary-keys
  "Returns the primary key columns for `identifier`."
  [db identifier]
  @(sql/select (schema-db db) [:key-column-usage.*]
     (sql/from :information-schema.table-constraints)
     (sql/join :information-schema.key-column-usage
               '(on (and (= :table-constraints.table-catalog
                            :key-column-usage.table-catalog)
                         (= :table-constraints.table-schema
                            :key-column-usage.table-schema)
                         (= :table-constraints.table-name
                            :key-column-usage.table-name)
                         (= :table-constraints.constraint-name
                            :key-column-usage.constraint-name))))
     (sql/where `(and (= :table-constraints.table-catalog ~(table-catalog db))
                      (= :table-constraints.table-schema ~(table-schema db identifier))
                      (= :table-constraints.table-name ~(table-name db identifier))
                      (= :table-constraints.constraint-type "PRIMARY KEY")))
     (sql/order-by :ordinal-position)))

(s/fdef primary-keys
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret (s/coll-of ::column))

(defn foreign-keys
  "Returns the foreign key columns for `identifier`."
  [db identifier]
  @(sql/select (schema-db db) [:table-constraints.*
                               :key-column-usage.column-name
                               (sql/as :constraint-column-usage.table-catalog
                                       :reference-table-catalog)
                               (sql/as :constraint-column-usage.table-schema
                                       :reference-table-schema)
                               (sql/as :constraint-column-usage.table-name
                                       :reference-table-name)
                               (sql/as :constraint-column-usage.column-name
                                       :reference-column-name)]
     (sql/from :information-schema.table-constraints)
     (sql/join :information-schema.key-column-usage
               '(on (and (= :table-constraints.table-catalog
                            :key-column-usage.table-catalog)
                         (= :table-constraints.table-schema
                            :key-column-usage.table-schema)
                         (= :table-constraints.constraint-name
                            :key-column-usage.constraint-name))))
     (sql/join :information-schema.referential-constraints
               '(on (and (= :table-constraints.table-catalog
                            :referential-constraints.constraint-catalog)
                         (= :table-constraints.table-schema
                            :referential-constraints.constraint-schema)
                         (= :table-constraints.constraint-name
                            :referential-constraints.constraint-name))))
     (sql/join :information-schema.constraint-column-usage
               '(on (and (= :table-constraints.table-catalog
                            :constraint-column-usage.constraint-catalog)
                         (= :table-constraints.table-schema
                            :constraint-column-usage.constraint-schema)
                         (= :table-constraints.constraint-name
                            :constraint-column-usage.constraint-name))))
     (sql/where `(and (= :table-constraints.table-catalog ~(table-catalog db))
                      (= :table-constraints.table-schema ~(table-schema db identifier))
                      (= :table-constraints.table-name ~(table-name db identifier))
                      (= :table-constraints.constraint-type "FOREIGN KEY")))
     (sql/order-by :ordinal-position)))

(s/fdef foreign-keys
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret (s/coll-of ::column))

(defn unique-keys
  "Returns the unique key columns for `identifier`."
  [db identifier]
  @(sql/select (schema-db db) [:table-constraints.* :column-name]
     (sql/from :information-schema.table-constraints)
     (sql/join :information-schema.key-column-usage
               '(on (and (= :table-constraints.table-catalog
                            :key-column-usage.table-catalog)
                         (= :table-constraints.table-schema
                            :key-column-usage.table-schema)
                         (= :table-constraints.constraint-name
                            :key-column-usage.constraint-name))))
     (sql/where `(and (= :table-constraints.table-catalog ~(table-catalog db))
                      (= :table-constraints.table-schema ~(table-schema db identifier))
                      (= :table-constraints.table-name ~(table-name db identifier))
                      (= :table-constraints.constraint-type "UNIQUE")))
     (sql/order-by :ordinal-position)))

(s/fdef unique-keys
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret (s/coll-of ::column))

(defn column
  "Returns the columns for `identifier`."
  [db identifier]
  (let [{:keys [schema table] :as column} (expr/parse-column identifier)]
    (first @(sql/select (schema-db db) [:columns.*]
              (sql/from :information-schema.columns)
              (sql/where `(and (= :table-catalog ~(table-catalog db))
                               (= :table-schema ~(or (some-> schema name) "public"))
                               (= :table-name ~(-> table name))
                               (= :column-name ~(-> column :name name))))
              (sql/order-by :ordinal-position)))))

(s/fdef column
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret (s/nilable ::column))

(defn columns
  "Returns the columns for `identifier`."
  [db identifier]
  @(sql/select (schema-db db) [:columns.*]
     (sql/from :information-schema.columns)
     (sql/where `(and (= :table-catalog ~(table-catalog db))
                      (= :table-schema ~(table-schema db identifier))
                      (= :table-name ~(table-name db identifier))))
     (sql/order-by :ordinal-position)))

(s/fdef columns
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret (s/coll-of ::column))

(defn table
  "Returns the `identifier`."
  [db identifier]
  (let [columns (columns db identifier)
        foreign-keys (foreign-keys db identifier)
        primary-keys (primary-keys db identifier)
        unique-keys (unique-keys db identifier)]
    {:column-names (mapv :column-name columns)
     :columns (zip-column-name columns)
     :foreign-keys (zip-column-name foreign-keys)
     :op :table
     :primary-keys (zip-column-name primary-keys)
     :table-name (table-name db identifier)
     :table-schema (table-schema db identifier)
     :unique-keys (zip-column-name unique-keys)}))

(s/fdef table
  :args (s/cat :db sql/db? :identifier ::identifier)
  :ret (s/nilable ::table))
