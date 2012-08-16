(ns database.test.meta
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.meta
        database.test))

;; SCHEMAS

(deftest test-lookup-schema
  (is (nil? (lookup-schema :unknown-schema)))
  (let [schema (make-schema :oauth)]
    (is (= schema (lookup-schema (:name schema))))))

(deftest test-make-schema
  (is (thrown? AssertionError (make-schema nil)))
  (is (= (make-schema :public) (make-schema "public")))
  (let [schema (make-schema :public)]
    (is (= :public (:name schema)))))

(deftest test-register-schema
  (let [schema (make-schema :oauth)]
    (is (= schema (register-schema schema)))
    (is (= schema (get @*schemas* (:name schema))))))

(deftest test-schema-key
  (is (= [:public] (schema-key (make-schema :public)))))
