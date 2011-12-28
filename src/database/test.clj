(ns database.test
  (:require [leiningen.env.core :as env])
  (:use clojure.test))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (env/with-environment :test
       ~@body)))

(defn load-environments
  "Load the environments."
  [& [name]]
  (env/load-environments
   {:name (or name "database")}
   "test-resources/init.clj"))
