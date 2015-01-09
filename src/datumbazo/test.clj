(ns datumbazo.test
  (:refer-clojure :exclude [distinct group-by])
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [datumbazo.core :refer :all]))

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

(defmulti create-test-table
  (fn [db table] table))

(defmethod create-test-table :empsalary [db table]
  (create-table db table
    (column :depname :varchar)
    (column :empno :bigint)
    (column :salary :int)
    (column :enroll-date :date)))

(defmulti insert-test-table
  (fn [db table] table))

(defmethod insert-test-table :empsalary [db table]
  (insert db table []
    (values
     [{:depname "develop" :empno 10 :salary 5200 :enroll-date #inst "2007-08-01"}
      {:depname "sales" :empno 1 :salary 5000 :enroll-date #inst "2006-10-01"}
      {:depname "personnel" :empno 5 :salary 3500 :enroll-date #inst "2007-12-10"}
      {:depname "sales" :empno 4 :salary 4800 :enroll-date #inst "2007-08-08"}
      {:depname "personnel" :empno 2 :salary 3900 :enroll-date #inst "2006-12-23"}
      {:depname "develop" :empno 7 :salary 4200 :enroll-date #inst "2008-01-01"}
      {:depname "develop" :empno 9 :salary 4500 :enroll-date #inst "2008-01-01"}
      {:depname "sales" :empno 3 :salary 4800 :enroll-date #inst "2007-08-01"}
      {:depname "develop" :empno 8 :salary 6000 :enroll-date #inst "2006-10-01"}
      {:depname "develop" :empno 11 :salary 5200 :enroll-date #inst "2007-08-15"}])))

(defmacro with-test-table [db table & body]
  `(let [db# ~db table# ~table]
     (try @(create-test-table db# table#)
          @(insert-test-table db# table#)
          ~@body
          (finally @(drop-table db# [table#])))))
