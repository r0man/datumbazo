(ns datumbazo.test.adapter
  (:require [datumbazo.util :as util])
  (:use clojure.test
        datumbazo.adapter)
  (:import datumbazo.adapter.MySQL
           datumbazo.adapter.PostgreSQL))

(deftest test-mysql
  (let [adapter (mysql "mysql://localhost/datumbazo")]
    (is (instance? MySQL adapter))
    (is (= "localhost" (:host adapter)))
    (is (= 3306 (:port adapter)))
    (is (= (util/current-user) (:user adapter)))
    (is (nil?  (:password adapter))))
  (let [adapter (mysql "mysql://tiger:scotch@localhost:100/datumbazo")]
    (is (instance? MySQL adapter))
    (is (= "localhost" (:host adapter)))
    (is (= 100 (:port adapter)))
    (is (= "tiger" (:username adapter)))
    (is (= "scotch" (:password adapter)))))

(deftest test-postgresql
  (let [adapter (postgresql "postgresql://localhost/datumbazo")]
    (is (instance? PostgreSQL adapter))
    (is (= "localhost" (:host adapter)))
    (is (= 5432 (:port adapter)))
    (is (= (util/current-user) (:user adapter)))
    (is (nil? (:password adapter))))
  (let [adapter (postgresql "postgresql://tiger:scotch@localhost:100/datumbazo")]
    (is (instance? PostgreSQL adapter))
    (is (= "localhost" (:host adapter)))
    (is (= 100 (:port adapter)))
    (is (= "tiger" (:username adapter)))
    (is (= "scotch" (:password adapter)))))
