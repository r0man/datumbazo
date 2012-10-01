(ns database.core
  (:require [clojure.java.jdbc :as jdbc]
            [environ.core :refer [env]]
            [slingshot.slingshot :refer [throw+]]))

(defmacro with-connection
  "Evaluates body in the context of a new connection to the `name`
  database. The connection spec for `name` is looked up via environ."
  [name & body]
  `(let [name# ~name]
     (if-let [spec# (env name#)]
       (jdbc/with-connection spec# ~@body)
       (throw+ {:type ::connection-not-found :name name#}))))

(defn make-table [name]
  {:table-name (keyword name)})
