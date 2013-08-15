(ns datumbazo.test
  (:require [clojure.test :refer [deftest]]
            [environ.core :refer [env]]
            [datumbazo.core :refer  [with-rollback]]
            [datumbazo.connection :refer  [connection]]))

;; (def db (connection (env :test-db)))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ^:integration ~name
     (with-rollback [~'db (env :test-db)]
       ~@body)))
