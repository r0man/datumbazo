(ns datumbazo.record
  (:refer-clojure :exclude [update])
  (:require [clojure.set :as set]
            [datumbazo.associations :as associations]
            [datumbazo.callbacks :as callback]
            [sqlingvo.core :refer [db?]]
            [datumbazo.util :as util]
            [inflections.core :as infl]
            [potemkin.collections :refer [def-map-type]]
            [sqlingvo.core :as sql :refer [column]]
            [clojure.spec.alpha :as s])
  (:import [java.util List Map]
           sqlingvo.db.Database))

(defmulti select-class
  "Return the SELECT statement for `class`. This statement will be
  used by all the finder functions that are generated for `class`."
  (fn [db class & [opts]] class))

(defmulti select-columns
  "Return the columns for `class` used in SELECT and RETURNING clauses."
  (fn [db class & [opts]] class))

(defmulti delete-records
  "Delete `records` of `class` in `db`."
  (fn [db class records & [opts]] class))

(defmulti insert-records
  "Insert `records` of `class` into `db`."
  (fn [db class records & [opts]] class))

(defmulti update-records
  "Update `records` of `class` in `db`."
  (fn [db class records & [opts]] class))

(defmulti save-records
  "Save `records` of `class` to `db`."
  (fn [db class records & [opts]] class))

(defmulti select-column-expr
  (fn [db table column] (:type column)))

(defmethod select-column-expr :geography [db table column]
  (sql/as `(cast ~(util/column-keyword column true) :geometry) (:name column)))

(defmethod select-column-expr :default [db table column]
  (util/column-keyword column true))

(defmethod select-class :default
  [db class & [opts]]
  (let [table (util/table-by-class class)
        columns (util/columns-by-class class)]
    (sql/select db (map #(select-column-expr db table %) columns)
      (sql/from (util/table-keyword table)))))

(defmethod select-columns :default
  [db class & [opts]]
  (let [table (util/table-by-class class)]
    (map #(select-column-expr db table %)
         (util/columns-by-class class))))

(defn- primary-key-columns
  "Return the primary key columns of `class`."
  [class]
  (let [table (util/table-by-class class)]
    (->> (vals (:column table))
         (filter :primary-key?)
         (set))))

(s/fdef primary-key-columns
  :args (s/cat :class class?))

(defn- unique-key-columns
  "Return the unique key columns of `class`."
  [class]
  (let [table (util/table-by-class class)]
    (->> (vals (:column table))
         (filter :unique?)
         (set))))

(s/fdef unique-key-columns
  :args (s/cat :class class?))

(defn- select-all-keys [map keyseq]
  (persistent! (reduce #(assoc! %1 %2 (get map %2))
                       (transient {}) keyseq)))

(defn select-values
  "Return a seq of `records` that only have the table column keys."
  [class records]
  (let [table-keys (set (map :form (util/columns-by-class class)))
        record-keys (set (mapcat keys records))
        keys (set/intersection table-keys record-keys)]
    (map #(util/record->row class (select-all-keys % keys)) records)))

(s/fdef select-values
  :args (s/cat :class class? :records (s/coll-of map?)))

(defn- returning-clause
  "Return a RETURNING clause for `class`."
  [db class]
  (apply sql/returning (select-columns db class)))

(s/fdef returning-clause
  :args (s/cat :db sql/db? :class class?))

(defmethod delete-records :default
  [db class records & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-delete records)
        table (util/table-by-class class)
        pk (first (primary-key-columns class))]
    (assert pk (str "No primary key found for " class))
    (->> @(sql/delete db (util/table-keyword table)
            (sql/where `(in ~(:name pk) ~(map (:form pk) records)))
            (returning-clause db class))
         (util/make-instances db class)
         (callback/call-after-delete))))

(defmethod insert-records :default
  [db class records & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-create records)
        table (util/table-by-class class)]
    (->> @(sql/insert db (util/table-keyword table) []
            (sql/values (select-values class records))
            (returning-clause db class))
         (util/make-instances db class)
         (callback/call-after-create))))

(defn- update-column
  "Return the qualified update column."
  [column]
  (keyword (str "update." (name (:name column)))))

(defn- update-columns
  "Return all columns of `class`, except the primary key columns."
  [class]
  (util/columns-by-class class))

(defn- update-expression
  "Return the update expression for `class` and `records`."
  [class records]
  (let [record-keys (set (mapcat keys records))]
    (->> (for [column (update-columns class)
               :when (contains? record-keys (:form column))]
           [(:name column) (update-column column)])
         (into {}))))

(s/fdef update-expression
  :args (s/cat :class class? :records (s/coll-of map?)))

(defn- update-condition
  "Return the update condition for `class`."
  [class]
  (->> (for [column (primary-key-columns class)]
         (list '= (util/column-keyword column true)
               (update-column column)))
       (concat ['and])))

(s/fdef update-condition
  :args (s/cat :class class?))

(defn- update-values
  "Return the update values for `class` and `records`."
  [class records]
  (map #(map (fn [{:keys [form] :as column}]
               `(cast ~(get % form) ~(util/cast-type column)))
             (util/columns-by-class class))
       records))

(s/fdef update-values
  :args (s/cat :class class? :records (s/coll-of map?)))

(defmethod update-records :default
  [db class records & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-update records)
        table (util/table-by-class class)
        columns (util/columns-by-class class)
        values (update-values class records)]
    (->> @(sql/update db (util/table-keyword table)
            (update-expression class records)
            (sql/from (sql/as (sql/values values) :update (map :name columns)))
            (sql/where (update-condition class))
            (returning-clause db class))
         (util/make-instances db class)
         (callback/call-after-update))))

(defn- on-conflict-clause
  "Return the ON CONFLICT clause for an insert statement."
  [class]
  (let [pk-columns (seq (primary-key-columns class))
        unique-columns (seq (unique-key-columns class))]
    (cond
      pk-columns
      (map :name pk-columns)
      unique-columns
      [(:name (first unique-columns))]
      :else
      (throw (ex-info (str "Can't guess ON CONFLICT clause, because "
                           class " has no primary key nor a unique constraint declared.")
                      {:class class})))))

(s/fdef on-conflict-clause
  :args (s/cat :class class?))

(defn- do-update-clause
  "Return the DO UPDATE clause for an insert statement."
  [class]
  (let [exclude (or (first (unique-key-columns class))
                    (first (primary-key-columns class)))]
    (->> (for [column (util/columns-by-class class)
               :when (not= (:name column) (:name exclude))
               :when (not (#{:bigserial :serial} (:type column)))
               :let [column-kw (:name column)]]
           [column-kw (keyword (str "EXCLUDED." (name column-kw)))])
         (into {}))))

(s/fdef do-update-clause
  :args (s/cat :class class?))

(defmethod save-records :default
  [db class records & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-save records)
        table (util/table-by-class class)]
    (->> @(sql/insert db (util/table-keyword table) []
            (sql/values (select-values class records))
            (sql/on-conflict (on-conflict-clause class)
              (sql/do-update (do-update-clause class)))
            (returning-clause db class))
         (util/make-instances db class)
         (callback/call-after-save))))

(defn row-get [record column default-value]
  (let [table (util/table-by-class (class record))
        association (get-in table [:associations column])]
    (cond
      (contains? (.attrs record) column)
      (get (.attrs record) column default-value)
      association
      (associations/fetch association record)
      :else nil)))

(defn- define-class
  "Define a map-like class for the rows in `table`."
  [table]
  (let [class (util/class-symbol table)]
    `(def-map-type ~class [~'db ~'attrs ~'meta-data]
       (~'get [~'this ~'column ~'default-value]
        (row-get ~'this ~'column ~'default-value))
       (~'assoc [~'this ~'column ~'value]
        (new ~class ~'db (assoc ~'attrs ~'column ~'value) ~'meta-data))
       (~'dissoc [~'this ~'column]
        (new ~class ~'db (dissoc ~'attrs ~'column) ~'meta-data))
       (~'keys [~'this]
        (keys ~'attrs))
       (~'meta [~'this]
        ~'meta-data)
       (~'empty [~'this]
        (new ~class ~'db {} nil))
       (~'with-meta [~'this ~'meta-data]
        (new ~class ~'db ~'attrs ~'meta-data)))))

(defn find-all
  "Select all rows of `class`."
  [db class & [opts]]
  (->> @(select-class db class opts)
       (util/make-instances db class)
       (callback/call-after-find)))

(s/fdef find-all
  :args (s/cat :db sql/db? :class class? :opts (s/? (s/nilable map?))))

(defn- coerce-unique [column value rows]
  (if (and (or (:unique? column) (:primary-key? column))
           (not (sequential? value)))
    (do (assert (empty? (next rows)) "Expected zero or one row, got many.")
        (first rows))
    rows))

(defn find-by-column
  [db class column-kw value & [opts]]
  (let [table (util/table-by-class class)
        column (get (:column table) column-kw)]
    (->> @(sql/compose
           (select-class db class)
           (sql/where
            `(in ~(util/column-keyword column true)
                 ~(for [value (if (sequential? value) value [value])]
                    `(cast ~value ~(util/cast-type column))))))
         (util/make-instances db class)
         (callback/call-after-find)
         (coerce-unique column value))))

(s/fdef find-by-column
  :args (s/cat :db sql/db?
               :class class?
               :column-kw keyword?
               :value any?
               :opts (s/? (s/nilable map?))))

(defn exists?
  "Returns true if the `record` of `class` can be found in `db`,
  otherwise false. Uses the primary key and unique column."
  [db class record]
  (let [columns (concat (primary-key-columns class)
                        (unique-key-columns class))]
    (assert (not (empty? columns)) "No primary key or unique columns.")
    (-> @(sql/select db [1]
           (sql/from (util/table-by-class class))
           (sql/where `(or ~@(for [column columns]
                               `(= ~column ~(get record (:name column)))))))
        empty? not)))

(s/fdef exists? :args (s/cat :db sql/db? :class class? :record map?))

(defn- define-exists?
  "Return the definition for the `exists?` function."
  [table]
  `(defn ~'exists?
     ~(str "Returns true if the `record` exists in `db`, otherwise false.")
     [~'db ~'record]
     (exists? ~'db ~(util/class-symbol table) ~'record)))

(defn- define-find-by-column
  "Return the definition for a function that returns rows by a column."
  [table column & [opts]]
  (let [table-kw (-> table :name name keyword)
        column-kw (-> column :name name keyword)
        column-sym (-> column :name name symbol)]
    `(defn ~(symbol (str "by-" (-> column :name name)))
       ~(str "Find all rows in `db` by `" (-> column :name name) "`.")
       [~'db ~column-sym & [~'opts]]
       (find-by-column ~'db ~(util/class-symbol table) ~column-kw ~column-sym ~'opts))))

(defn- define-insert
  "Define a function that inserts records into `table`."
  [table]
  `(do (defn ~'insert-all!
         "Insert all `records` into the `db`."
         [~'db ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (insert-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (defn ~'insert!
         "Insert `record` into the `db`."
         [~'db ~'record & [~'opts]]
         {:pre [(db? ~'db)]}
         (first (~'insert-all! ~'db [~'record] ~'opts)))))

(defn- define-delete
  "Define a function that deletes records in `table`."
  [table]
  `(do (defn ~'delete-all!
         "Delete all `records` from `db`."
         [~'db ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (delete-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (defn ~'delete!
         "Delete the `record` from `db`."
         [~'db ~'record & [~'opts]]
         {:pre [(db? ~'db)]}
         (first (~'delete-all! ~'db [~'record] ~'opts)))))

(defn- define-update
  "Define a function that updates records in `table`."
  [table]
  `(do (defn ~'update-all!
         "Update all `records` in the `db`."
         [~'db ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (update-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (defn ~'update!
         "Update `record` in the `db`."
         [~'db ~'record & [~'opts]]
         {:pre [(db? ~'db)]}
         (first (~'update-all! ~'db [~'record] ~'opts)))))

(defn- define-save
  "Define a function that saves records to `table`."
  [table]
  `(do (defn ~'save-all!
         "Save all `records` to `db`."
         [~'db ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (save-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (defn ~'save!
         "Save `record` to `db`."
         [~'db ~'record & [~'opts]]
         {:pre [(db? ~'db)]}
         (first (~'save-all! ~'db [~'record] ~'opts)))))

(defn- define-instance?
  "Define a function that checks if a record is an instance of a class."
  [table]
  (let [entity (infl/singular (-> table :name name))]
    `(defn ~(symbol (str entity "?"))
       ~(str "Return true if `x` is a " entity ", otherwise false.")
       [~'x]
       (instance? ~(util/class-symbol table) ~'x))))

(defn- define-find-all
  "Return the definition for a function that returns all rows."
  [table & [opts]]
  (let [table-kw (-> table :name name keyword)]
    `(defn ~'all
       "Find all rows in `db`."
       [~'db & [~'opts]]
       {:pre [(db? ~'db)]}
       (find-all ~'db ~(util/class-symbol table) ~'opts))))

(defn define-make-instance
  "Define the `make-instance` multi method for `table`."
  [table]
  (let [class (util/class-symbol table)]
    `(defmethod datumbazo.util/make-instance ~class
       [~'class ~'attrs & [~'db]]
       (-> (new ~class ~'db (util/row->record ~class ~'attrs) (meta ~'attrs))
           (callback/after-initialize)))))

(defn define-record [table]
  `(do ~(define-class table)
       ~(define-exists? table)
       ~(define-instance? table)
       ~(define-delete table)
       ~(define-insert table)
       ~(define-update table)
       ~(define-save table)
       ~(define-find-all table)
       ~(define-make-instance table)
       ~@(for [column# (vals (:column table))]
           (define-find-by-column table column#))))
