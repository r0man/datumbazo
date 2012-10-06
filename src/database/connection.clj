(ns database.connection
  (:require [clojure.java.jdbc :as jdbc]
            [environ.core :refer [env]]
            [inflections.core :refer [dasherize underscore]]))

(def ^:dynamic *naming-strategy*
  {:entity underscore :keyword dasherize})

(defn resolve-connection
  [db-spec]
  (if (keyword? db-spec)
    (or (env db-spec)
        (throw (IllegalArgumentException. (format "Can't resolve connection spec via environ: %s" db-spec))))
    db-spec))

(defmacro with-connection
  "Evaluates body in the context of a connection to the database
  `name`. The connection spec for `name` is looked up via environ."
  [name & body]
  `(jdbc/with-naming-strategy *naming-strategy*
     (jdbc/with-connection (resolve-connection ~name)
       ~@body)))

(defn wrap-connection
  "Returns a Ring handler with that has a connection to the `db-spec`."
  [handler db-spec]
  (fn [request]
    (with-connection db-spec
      (handler request))))
