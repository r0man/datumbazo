(ns database.test
  (:require [clojure.java.jdbc :as jdbc]
            [leiningen.env.core :as env])
  (:use clojure.test
        database.connection))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (env/with-environment :test
       (with-connection :default
         (jdbc/transaction (try ~@body (finally (jdbc/set-rollback-only))))))))

(defn load-environments
  "Load the environments."
  [& [name]]
  (env/load-environments
   {:name (or name "database")}
   "test-resources/init.clj"))
