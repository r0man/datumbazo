(ns datumbazo.test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [datumbazo.core :refer  [with-rollback]]
            [datumbazo.connection :refer  [connection]]))

(def connections
  {:mysql "mysql://tiger:scotch@localhost/datumbazo"
   :postgresql "postgresql://tiger:scotch@localhost/datumbazo"
   :sqlite "sqlite://tmp/datumbazo.db"})

(def db (connection (env :test-db)))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ^:integration ~name
     (with-rollback [~'db db]
       ~@body)))

(defmacro defvendor-test
  [test-name & body]
  `(deftest ~test-name
     ~@(for [[vendor# url#] connections]
         `(testing ~(str test-name " (" (name vendor#) ")")
            (let [~'vendor ~vendor#]
              (with-rollback [~'db ~url#]
                ~@body))))))
