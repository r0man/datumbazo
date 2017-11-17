(ns datumbazo.types
  (:require [clojure.spec.alpha :as s]))

(defn- spec-name
  "Returns the `db` specific spec name for `type`."
  [db type]
  (str "datumbazo." (-> db :scheme name) ".types/" (name type)))

(defn spec
  "Returns the `db` specific spec for `type`."
  [db type]
  (try (s/spec (keyword (spec-name db type)))
       (catch Exception e
         (throw (ex-info "Can't resolve spec for type."
                         {:type type})))))

(defn gen
  "Returns the `db` specific generator for `type`."
  [db type]
  (s/gen (spec db type)))
