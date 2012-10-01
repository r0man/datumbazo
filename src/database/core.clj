(ns database.core
  (:require [clojure.java.jdbc :as jdbc]
            [environ.core :refer [env]]))

(defmacro with-connection
  "Evaluates body in the context of a new connection to the `name`
  database. The connection spec for `name` is looked up via environ."
  [name & body]
  `(let [name# ~name]
     (jdbc/with-connection
       (if (keyword? name#)
         (or (env name#)
             (throw (IllegalArgumentException. (format "Can't resolve connection spec via environ: %s" name#))))
         name#)
       ~@body)))

(defn make-table [name]
  {:table-name (keyword name)})
