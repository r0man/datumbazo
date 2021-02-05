(ns datumbazo.types
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [sqlingvo.core :as sql]))

(defprotocol IType
  (-type-name [x]))

(defn- spec-name
  "Returns the `db` specific spec name for `type`."
  [db type]
  (str "datumbazo.db." (-> db :scheme name) ".type/" (-type-name type)))

(s/fdef spec-name
  :args (s/cat :db sql/db? :type any?)
  :ret string?)

(defn- spec-keyword
  "Returns the `db` specific spec name for `type`."
  [db type]
  (keyword (spec-name db type)))

(s/fdef spec-keyword
  :args (s/cat :db sql/db? :type any?)
  :ret keyword?)

(defn spec
  "Returns the `db` specific spec for `type-or-column`."
  [db type-or-column & [opts]]
  (when-let [spec (s/get-spec (spec-keyword db type-or-column))]
    (cond-> spec
      (or (:nilable? opts)
          (:nil? type-or-column)
          (= (:is-nullable type-or-column) "YES"))
      (s/nilable))))

(defn gen
  "Returns the `db` specific generator for `type`."
  [db type]
  (-> (spec db type) s/gen))

(extend-protocol IType

  clojure.lang.Keyword
  (-type-name [k]
    (-type-name (name k)))

  clojure.lang.PersistentHashMap
  (-type-name [{:keys [data-type udt-name]}]
    (cond
      (= data-type "USER-DEFINED")
      (-type-name udt-name)
      (string? data-type)
      (-type-name data-type)))

  String
  (-type-name [s]
    (-> (str/lower-case s)
        (str/replace #"\s+" "-"))))
