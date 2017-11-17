(ns datumbazo.gen
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.test.check.generators :as gens]
            [datumbazo.meta :as meta]
            [datumbazo.types :as types]
            [sqlingvo.core :as sql]
            [sqlingvo.expr :as expr]))

(defn varchar [size]
  (gen/fmap #(apply str %) (gen/vector (gen/char-alpha) size)))

(s/fdef varchar
  :args (s/cat :size nat-int?)
  :ret gens/generator?)

(defn- column->gen
  [db {:keys [type-name is-nullable] :as column}]
  (cond-> (types/spec db type-name)
    (= is-nullable "YES") s/nilable
    true s/gen))

(s/fdef column->gen
  :args (s/cat :db sql/db? :column map?)
  :ret gens/generator?)

(defn column
  "Returns a generator that produces values for `column` in `db`."
  [db column]
  (some->> (meta/column db column) (column->gen db)))

(s/fdef column
  :args (s/cat :db sql/db? :table keyword?)
  :ret (s/nilable gens/generator?))

(defn row
  "Returns a generator that produces rows for `table` in `db`."
  [db table]
  (when-let [{:keys [schema name]} (expr/parse-table table)]
    (->> (meta/columns db {:schema schema :table name})
         (mapcat #(vector (-> % :column-name keyword) (column->gen db %)))
         (apply gen/hash-map))))

(s/fdef row
  :args (s/cat :db sql/db? :table keyword?)
  :ret (s/nilable gens/generator?))
