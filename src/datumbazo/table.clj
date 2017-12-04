(ns datumbazo.table
  (:require [clojure.spec.alpha :as s]
            [datumbazo.gen :as generators]
            [datumbazo.associations :as a]
            [datumbazo.postgresql.types :as types]
            [datumbazo.record :as record]
            [datumbazo.util :as util]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]
            [inflections.core :as i]
            [clojure.string :as str]
            [clojure.test.check.generators :as gen]))

(def belongs-to a/belongs-to)
(def has-many a/has-many)
(def column sql/column)

(defn columns
  "Returns the columns of `table`."
  [table]
  (map (:column table) (:columns table)))

(defn table
  "Make a new table."
  [name & body]
  (fn [table]
    [nil (merge
          table
          (second
           ((sql/chain-state body)
            (expr/parse-table name))))]))

(defn define-table-by-class
  "Define the `table-by-class` multi method for `table`."
  [table]
  (let [class (util/class-symbol table)]
    `(defmethod datumbazo.util/table-by-class ~class
       [~'class]
       (quote ~table))))

(defn- define-truncate
  "Define a function that truncates `table`."
  [table]
  `(defn ~'truncate!
     ~(str "Truncate the " (-> table :name name) " table.")
     [~'db & [~'opts]]
     @(sql/truncate ~'db [~(util/table-keyword table)]
        (sql/cascade (:cascade ~'opts)))))

(defn- column-spec-ns
  "Return the name of the `column` spec in `ns`."
  [table column]
  (str (str *ns*) "." (-> table :name name i/singular)))

(defn- column-spec-name
  "Return the name of the `column` spec in `ns`."
  [table column]
  (keyword (column-spec-ns table column)
           (-> column :name name)))

(defn- column-spec-type
  "Return the type of the `column` in the `datumbazo.types` namespace."
  [column]
  (keyword "datumbazo.postgresql.types" (-> column :type name)))

(defmulti column-spec-gen
  "Return the generator for a column spec."
  (fn [table column] (:type column)))

(defmethod column-spec-gen :varchar [table column]
  `(s/with-gen (s/and string? #(= (count %1) ~(:size column)))
     #(generators/varchar ~(:size column))))

(defmethod column-spec-gen :default [table column]
  (column-spec-type column))

(defn- column-spec
  "Define a function that truncates `table`."
  [table column]
  `(s/def ~(column-spec-name table column)
     ~(column-spec-gen table column)))

(defn- define-column-specs
  "Define a function that truncates `table`."
  [table]
  `(do ~@(mapv #(column-spec table %) (columns table))))

(defn- row-spec-name
  "Return the name of the `row` spec in `ns`."
  [table]
  (let [singular (-> table :id name i/singular)]
    (if-let [ns (-> table :id namespace)]
      (keyword ns singular)
      (keyword (str *ns*) singular))))

(defn- row-spec-req
  "Return the required row spec keys in a vector."
  [table]
  (->> (columns table)
       (filter :ns)
       (filter :primary-key?)
       (mapv #(column-spec-name table %1))))

(defn- row-spec-req-un
  "Return the required row spec keys in a vector."
  [table]
  (->> (columns table)
       (filter (complement :ns))
       (filter :primary-key?)
       (mapv #(column-spec-name table %1))))

(defn- row-spec-opt
  "Return the optional row spec keys in a vector."
  [table]
  (->> (columns table)
       (filter :ns)
       (remove :primary-key?)
       (remove :not-null?)
       (mapv #(column-spec-name table %1))))

(defn- row-spec-opt-un
  "Return the optional row spec keys in a vector."
  [table]
  (->> (columns table)
       (filter (complement :ns))
       (remove :primary-key?)
       (mapv #(column-spec-name table %1))))

(defn- define-row-spec
  "Define a function that truncates `table`."
  [table]
  `(s/def ~(row-spec-name table)
     (s/keys
      :opt ~(row-spec-opt table)
      :opt-un ~(row-spec-opt-un table)
      :req ~(row-spec-req table)
      :req-un ~(row-spec-req-un table))))

(defn- row-gen-req-un
  "Return the required row spec for a row generator."
  [table]
  (->> (columns table)
       (filter (complement :ns))
       (mapv #(column-spec-name table %1))))

(defn- define-gen
  "Define a generator for rows in `table`."
  [table]
  `(defn ~'gen
     ~(str "A generator that produces rows for the " (:name table) " table.")
     ([] (s/gen (s/keys :req-un ~(row-gen-req-un table))))
     ([~'db] (s/gen (s/keys :req-un ~(row-gen-req-un table))))))

(defmacro deftable
  "Define a database table."
  [table-name doc & body]
  (let [table# (eval `(second ((table ~(keyword table-name) ~@body) {})))
        table# (assoc table# :id table-name)]
    `(do ~(record/define-record table#)
         ~(define-table-by-class table#)
         ~(define-truncate table#)
         ~(define-column-specs table#)
         ~(define-row-spec table#)
         ~(define-gen table#))))
