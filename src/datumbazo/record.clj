(ns datumbazo.record
  (:refer-clojure :exclude [update])
  (:require [clojure.set :as set]
            [datumbazo.associations :as associations]
            [datumbazo.callbacks :as callback]
            [datumbazo.util :as util]
            [inflections.core :as infl]
            [potemkin.collections :refer [def-map-type]]
            [schema.core :as s]
            [sqlingvo.core :as sql :refer [column]])
  (:import [java.util List Map]
           sqlingvo.db.Database))

(s/defn ^:private primary-key-columns :- #{Map}
  "Return the primary key columns of `class`."
  [class :- Class]
  (let [table (util/table-by-class class)]
    (->> (vals (:column table))
         (filter :primary-key?)
         (set))))

(s/defn ^:private select-columns :- [Map]
  "Return a seq of `records` that only have the table column keys."
  [class :- Class records :- [Map]]
  (let [keys (map :name (util/columns-by-class class))]
    (map #(select-keys % keys) records)))

(s/defn delete-records :- [Map]
  "Delete `records` of `class` from `db`."
  [db :- Database class :- Class records :- [Map]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-delete records)
        table (util/table-by-class class)
        pk (first (primary-key-columns class))]
    (assert pk (str "No primary key found for " class))
    (->> @(sql/delete db (util/table-keyword table)
            (sql/where `(in ~(:name pk) ~(map :id records)))
            (sql/returning :*))
         (util/make-instances db class)
         (callback/call-after-delete))))

(s/defn insert-records :- [Map]
  "Insert `records` of `class` into `db`."
  [db :- Database class :- Class records :- [Map]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-create records)
        table (util/table-by-class class)]
    (->> @(sql/insert db (util/table-keyword table) []
            (sql/values (select-columns class records))
            (sql/returning :*))
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
         (list '= (keyword (str (name (:table column)) "." (name (:name column))))
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

(s/defn update-records :- [Map]
  "Update `records` of `class` in `db`."
  [db :- Database class :- Class records :- [Map]]
  (let [records (util/make-instances db class records)
        records (callback/call-before-update records)
        table (util/table-by-class class)
        columns (util/columns-by-class class)
        values (update-values class records)]
    (->> @(sql/update db (util/table-keyword table)
            (update-expression class records)
            (sql/from (sql/as (sql/values values) :update (map :name columns)))
            (sql/where (update-condition class))
            (sql/returning :*))
         (util/make-instances db class)
         (callback/call-after-update))))

(defn row-get [record column default-value]
  (let [table (util/table-by-class (class record))]
    (if-let [association (-> table :associations column)]
      (associations/fetch association record)
      (get (.attrs record) column default-value))))

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
  [db class & [opts]]
  (let [table (util/table-by-class class)]
    (->> @(sql/select db [:*]
            (sql/from (util/table-keyword table)))
         (util/make-instances db class)
         (callback/call-after-find))))

(defn- coerce-unique [column rows]
  (if (or (:unique? column)
          (:primary-key? column))
    (do (assert (empty? (next rows)) "Expected zero or one row, got many.")
        (first rows))
    rows))

(defn find-by-column
  [db class column value & [opts]]
  (let [table (util/table-by-class class)]
    (->> @(sql/select db [:*]
            (sql/from (util/table-keyword table))
            (sql/where `(= ~column ~value)))
         (util/make-instances db class)
         (callback/call-after-find)
         (coerce-unique (get (:column table) column)))))

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
  `(defn ~'insert!
     "Insert `records` into the `db`."
     [~'db ~'records]
     (insert-records ~'db ~(util/class-symbol table) ~'records)))

(defn- define-delete
  "Define a function that deletes records in `table`."
  [table]
  `(defn ~'delete!
     "Delete `records` from `db`."
     [~'db ~'records]
     (delete-records ~'db ~(util/class-symbol table) ~'records)))

(defn- define-update
  "Define a function that updates records in `table`."
  [table]
  `(defn ~'update!
     "Update `records` in the `db`."
     [~'db ~'records]
     (update-records ~'db ~(util/class-symbol table) ~'records)))

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
       ~(define-find-all table)
       ~(define-make-instance table)
       ~@(for [column# (vals (:column table))]
           (define-find-by-column table column#))))
