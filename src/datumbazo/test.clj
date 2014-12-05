(ns datumbazo.test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [datumbazo.core :refer  [with-db]]
            [datumbazo.db :refer  [new-db]]))

(def connections
  {:mysql "mysql://tiger:scotch@localhost/datumbazo"
   :postgresql "postgresql://tiger:scotch@localhost/datumbazo"
   :sqlite "sqlite://tmp/datumbazo.db"})

(def db (new-db (:postgresql connections)))

(defmacro with-test-db
  [[db-sym connection] & body]
  `(let [db# (new-db (or ~connection ~(:postgresql connections)))]
     (with-db [~db-sym (assoc db# :test true)]
       ~@body)))

(defmacro with-test-dbs
  [[db-sym vendors] & body]
  `(do ~@(for [[db# url#] connections
               :when (or (empty? vendors)
                         (contains? vendors db#))]
           `(testing ~(name db#)
              (with-test-db [~'db ~url#]
                ~@body)))))
