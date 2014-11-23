(ns datumbazo.test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [environ.core :refer [env]]
            [datumbazo.connection :refer  [connection with-db]]))

(def connections
  {:mysql "mysql://tiger:scotch@localhost/datumbazo"
   :postgresql "postgresql://tiger:scotch@localhost/datumbazo"
   :sqlite "sqlite://tmp/datumbazo.db"})

(def db (connection (env :test-db)))

(defmacro with-test-db
  [[db-sym] & body]
  `(with-db [~db-sym (assoc db :test true)]
     ~@body))

(defmacro database-test-all
  [test-name & body]
  `(deftest ~test-name
     ~@(for [[db# url#] connections]
         `(testing ~(str test-name " (" (name db#) ")")
            (with-db [~'db (assoc (connection ~url#) :test true)]
              ~@body)))))
