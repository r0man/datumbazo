(ns datumbazo.record
  (:refer-clojure :exclude [update])
  (:require [clojure.set :as set]
            [datumbazo.associations :as associations]
            [datumbazo.callbacks :as callback]
            [sqlingvo.core :refer [db?]]
            [datumbazo.util :as util]
            [inflections.core :as infl]
            [potemkin.collections :refer [def-map-type]]
            [schema.core :as s]
            [sqlingvo.core :as sql :refer [column]])
  (:import [java.util List Map]
           sqlingvo.db.Database))

(defmulti select-class
  "Return the SELECT statement for `class`. This statement will be
  used by all the finder functions that are generated for `class`."
  (s/fn [db :- Database class :- Class & [opts]]
    class))

(defmulti delete-records
  "Delete `records` of `class` in `db`."
  (s/fn [db :- Database class :- Class records :- [Map] & [opts]]
    class))

(defmulti insert-records
  "Insert `records` of `class` into `db`."
  (s/fn [db :- Database class :- Class records :- [Map] & [opts]]
    class))

(defmulti update-records
  "Update `records` of `class` in `db`."
  (s/fn [db :- Database class :- Class records :- [Map] & [opts]]
    class))

(defmulti save-records
  "Save `records` of `class` to `db`."
  (s/fn [db :- Database class :- Class records :- [Map] & [opts]]
    class))

(s/defmethod select-class :default
  [db :- Database class :- Class & [opts]]
  (let [table (util/table-by-class class)
        columns (util/columns-by-class class)]
    (sql/select db (map #(util/column-keyword % true) columns)
      (sql/from (util/table-keyword table)))))

(s/defn ^:private primary-key-columns :- #{Map}
  "Return the primary key columns of `class`."
  [class :- Class]
  (let [table (util/table-by-class class)]
    (->> (vals (:column table))
         (filter :primary-key?)
         (set))))

(s/defn ^:private unique-key-columns :- #{Map}
  "Return the unique key columns of `class`."
  [class :- Class]
  (let [table (util/table-by-class class)]
    (->> (vals (:column table))
         (filter :unique?)
         (set))))

(s/defn ^:private select-columns :- [Map]
  "Return a seq of `records` that only have the table column keys."
  [class :- Class records :- [Map]]
  (let [keys (map :name (util/columns-by-class class))]
    (map #(select-keys % keys) records)))

(s/defn ^:private returning-clause
  "Return a RETURNING clause for `class`."
  [class]
  (->> (util/columns-by-class class)
       (map #(util/column-keyword % true))
       (apply sql/returning)))

(s/defmethod delete-records :default
  [db :- Database class :- Class records :- [Map] & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-delete records)
        table (util/table-by-class class)
        pk (first (primary-key-columns class))]
    (assert pk (str "No primary key found for " class))
    (->> @(sql/delete db (util/table-keyword table)
            (sql/where `(in ~(:name pk) ~(map :id records)))
            (returning-clause class))
         (util/make-instances db class)
         (callback/call-after-delete))))

(s/defmethod insert-records :default
  [db :- Database class :- Class records :- [Map] & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-create records)
        table (util/table-by-class class)]
    (->> @(sql/insert db (util/table-keyword table) []
            (sql/values (select-columns class records))
            (returning-clause class))
         (util/make-instances db class)
         (callback/call-after-create))))

(s/defn ^:private update-column :- s/Keyword
  "Return the qualified update column."
  [column :- Map]
  (keyword (str "update." (name (:name column)))))

(s/defn ^:private update-columns :- #{Map}
  "Return all columns of `class`, except the primary key columns."
  [class :- Class]
  (set/difference
   (util/columns-by-class class)
   (primary-key-columns class)))

(s/defn ^:private update-expression :- Map
  "Return the update expression for `class` and `records`."
  [class :- Class records :- [Map]]
  (let [record-keys (set (mapcat keys records))]
    (->> (for [column (update-columns class)
               :when (contains? record-keys (:name column))]
           [(:name column) (update-column column)])
         (into {}))))

(s/defn ^:private update-condition :- List
  "Return the update condition for `class`."
  [class :- Class]
  (->> (for [column (primary-key-columns class)]
         (list '= (util/column-keyword column true)
               (update-column column)))
       (concat ['and])))

(s/defn ^:private update-values :- [[s/Any]]
  "Return the update values for `class` and `records`."
  [class :- Class records :- [Map]]
  (map #(map (fn [{:keys [name type]}]
               `(cast ~(get % name)
                      ~(case type
                         :serial :integer
                         type)))
             (util/columns-by-class class))
       records))

(s/defmethod update-records :default
  [db :- Database class :- Class records :- [Map] & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-update records)
        table (util/table-by-class class)
        columns (util/columns-by-class class)
        values (update-values class records)]
    (->> @(sql/update db (util/table-keyword table)
            (update-expression class records)
            (sql/from (sql/as (sql/values values) :update (map :name columns)))
            (sql/where (update-condition class))
            (returning-clause class))
         (util/make-instances db class)
         (callback/call-after-update))))

(s/defn ^:private on-conflict-clause :- [s/Keyword]
  "Return the ON CONFLICT clause for an insert statement."
  [class :- Class]
  ;; TODO: Be more clever. Handle multiple PKs and unique constraints.
  (if-let [column (or (first (unique-key-columns class))
                      (first (primary-key-columns class)))]
    [(:name column)]
    (throw (ex-info (str "Can't guess ON CONFLICT clause, because "
                         class " has no primary key nor a unique constraint declared.")
                    {:class class}))))

(s/defn ^:private do-update-clause ; :- Map
  "Return the DO UPDATE clause for an insert statement."
  [class :- Class]
  (let [exclude (or (first (unique-key-columns class))
                    (first (primary-key-columns class)))]
    (->> (for [column (util/columns-by-class class)
               :when (not= (:name column) (:name exclude))
               :when (not (#{:bigserial :serial} (:type column)))
               :let [column-kw (:name column)]]
           [column-kw (keyword (str "EXCLUDED." (name column-kw)))])
         (into {}))))

(s/defmethod save-records :default
  [db :- Database class :- Class records :- [Map] & [opts]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-save records)
        table (util/table-by-class class)]
    (->> @(sql/insert db (util/table-keyword table) []
            (sql/values (select-columns class records))
            (sql/on-conflict (on-conflict-clause class)
              (sql/do-update (do-update-clause class)))
            (returning-clause class))
         (util/make-instances db class)
         (callback/call-after-save))))

(defn row-get [record column default-value]
  (let [table (util/table-by-class (class record))
        association (-> table :associations column)]
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

(s/defn find-all
  "Select all rows of `class`."
  [db :- Database class :- Class & [opts]]
  (->> @(select-class db class opts)
       (util/make-instances db class)
       (callback/call-after-find)))

(defn- coerce-unique [column rows]
  (if (or (:unique? column)
          (:primary-key? column))
    (do (assert (empty? (next rows)) "Expected zero or one row, got many.")
        (first rows))
    rows))

(defn find-by-column
  [db class column-kw value & [opts]]
  (let [table (util/table-by-class class)
        column (get (:column table) column-kw)]
    (->> @(sql/compose
           (select-class db class)
           (sql/where `(= ~(util/column-keyword column true) ~value)))
         (util/make-instances db class)
         (callback/call-after-find)
         (coerce-unique column))))

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
  `(do (s/defn ~'insert-all!
         "Insert all `records` into the `db`."
         [~'db :- Database ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (insert-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (s/defn ~'insert!
         "Insert `record` into the `db`."
         [~'db :- Database ~'record & [~'opts]]
         {:pre [(db? ~'db)]}
         (first (~'insert-all! ~'db [~'record] ~'opts)))))

(defn- define-delete
  "Define a function that deletes records in `table`."
  [table]
  `(do (s/defn ~'delete-all!
         "Delete all `records` from `db`."
         [~'db :- Database ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (delete-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (s/defn ~'delete!
         "Delete the `record` from `db`."
         [~'db :- Database ~'record & [~'opts]]
         {:pre [(db? ~'db)]}
         (first (~'delete-all! ~'db [~'record] ~'opts)))))

(defn- define-update
  "Define a function that updates records in `table`."
  [table]
  `(do (s/defn ~'update-all!
         "Update all `records` in the `db`."
         [~'db :- Database ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (update-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (s/defn ~'update!
         "Update `record` in the `db`."
         [~'db :- Database ~'record & [~'opts]]
         {:pre [(db? ~'db)]}
         (first (~'update-all! ~'db [~'record] ~'opts)))))

(defn- define-save
  "Define a function that saves records to `table`."
  [table]
  `(do (s/defn ~'save-all!
         "Save all `records` to `db`."
         [~'db :- Database ~'records & [~'opts]]
         {:pre [(db? ~'db)]}
         (save-records ~'db ~(util/class-symbol table) ~'records ~'opts))
       (s/defn ~'save!
         "Save `record` to `db`."
         [~'db :- Database ~'record & [~'opts]]
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
    `(s/defn ~'all
       "Find all rows in `db`."
       [~'db :- Database & [~'opts]]
       {:pre [(db? ~'db)]}
       (find-all ~'db ~(util/class-symbol table) ~'opts))))

(defn define-make-instance
  "Define the `make-instance` multi method for `table`."
  [table]
  (let [class (util/class-symbol table)]
    `(defmethod datumbazo.util/make-instance ~class
       [~'class ~'attrs & [~'db]]
       (-> (new ~class ~'db ~'attrs (meta ~'attrs))
           (callback/after-initialize)))))

(defn define-record [table]
  `(do ~(define-class table)
       ~(define-instance? table)
       ~(define-delete table)
       ~(define-insert table)
       ~(define-update table)
       ~(define-save table)
       ~(define-find-all table)
       ~(define-make-instance table)
       ~@(for [column# (vals (:column table))]
           (define-find-by-column table column#))))
