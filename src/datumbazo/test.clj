(ns datumbazo.test
  (:require [clojure.test :refer [deftest]]
            [environ.core :refer [env]]
            [sqlingvo.core :refer  [with-rollback]]))

(def db (env :test-db))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ^:integration ~name
     (with-rollback [~'db db]
       ~@body)))
