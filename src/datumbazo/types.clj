(ns datumbazo.types
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]))

(defn- spec-name
  "Returns the `db` specific spec name for `type`."
  [db type]
  (str "datumbazo." (-> db :scheme name) ".types/"
       (-> type name str/lower-case)))

(defn spec
  "Returns the `db` specific spec for `type`."
  [db type]
  (let [spec (keyword (spec-name db type))]
    (try (s/spec spec)
         (catch Exception e
           (throw (ex-info "Can't resolve spec for type."
                           {:type type :spec spec}))))))

(defn gen
  "Returns the `db` specific generator for `type`."
  [db type]
  (s/gen (spec db type)))
