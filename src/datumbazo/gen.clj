(ns datumbazo.gen
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test.check.generators :as gens]
            [datumbazo.information-schema :as schema]
            [datumbazo.types :as types]
            [datumbazo.util :as util]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]))

(defn varchar [size]
  (gen/fmap #(apply str %) (gen/vector (gen/char-alpha) size)))

(s/fdef varchar
  :args (s/cat :size nat-int?)
  :ret gens/generator?)

(defn- contains-value?
  [db table column value]
  (->> @(sql/select db [1]
          (sql/from (util/table-keyword table))
          (sql/where `(= ~(util/column-keyword column) ~value)))
       empty? not))

(defn- primary-key
  [table column-name]
  (get-in table [:primary-keys column-name]))

(defn- unique-key
  [table column-name]
  (get-in table [:unique-keys column-name]))

(defn- column-name
  [column]
  (or (-> column :column-name)
      (-> column :name name)))

(defn- satisfies-unique-constraint?
  [db table column gen]
  (let [values (atom #{})]
    (gen/such-that
     #(and (not (contains-value? db table column %))
           (if (contains? @values %)
             false
             (swap! values conj %)))
     gen 100)))

(s/fdef satisfies-unique-constraint?
  :args (s/cat :db sql/db?
               :table ::schema/table
               :column ::schema/column
               :gen gens/generator?)
  :ret gens/generator?)

(defn- column->gen
  [db table column]
  (let [column-name (column-name column)
        column (get-in table [:columns column-name])]
    (cond->> (types/gen db column)
      (or (primary-key table column-name)
          (unique-key table column-name))
      (satisfies-unique-constraint? db table column))))

(s/fdef column->gen
  :args (s/cat :db sql/db? :table ::schema/table :column map?)
  :ret gens/generator?)

(defn column
  "Returns a generator that produces column values for `identifier` in `db`."
  [db identifier]
  (let [column (expr/parse-column identifier)
        table (schema/table db (util/table-keyword column))]
    (column->gen db table column)))

(s/fdef column
  :args (s/cat :db sql/db? :table keyword?)
  :ret (s/nilable gens/generator?))

(defn row
  "Returns a generator that produces rows for `table` in `db`."
  [db table]
  (let [table (schema/table db table)]
    (->> (map (:columns table) (:column-names table))
         (mapcat #(vector (-> % :column-name keyword)
                          (column->gen db table %)))
         (apply gen/hash-map))))

(s/fdef row
  :args (s/cat :db sql/db? :table keyword?)
  :ret (s/nilable gens/generator?))
