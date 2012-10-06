(ns database.connection
  (:require [clojure.java.jdbc :as jdbc]
            [environ.core :refer [env]]
            [inflections.core :refer [dasherize underscore]]))

(def ^:dynamic *naming-strategy*
  {:entity underscore :keyword dasherize})

(defmacro with-connection
  "Evaluates body in the context of a new connection to the `name`
  database. The connection spec for `name` is looked up via environ."
  [name & body]
  `(let [name# ~name]
     (jdbc/with-naming-strategy *naming-strategy*
       (jdbc/with-connection
         (if (keyword? name#)
           (or (env name#)
               (throw (IllegalArgumentException. (format "Can't resolve connection spec via environ: %s" name#))))
           name#)
         ~@body))))

(defn wrap-connection
  "Returns a Ring handler with that has a connection to the `db-spec`."
  [handler db-spec]
  (fn [request]
    (with-connection db-spec
      (handler request))))
