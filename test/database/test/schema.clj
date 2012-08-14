(ns database.test.schema
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.connection
        database.schema
        database.protocol))

(deftest test-make-schema
  (let [schema (make-schema :public [{:name :continents} {:name :countries}])]
    (is (= :public (:name schema)))
    (is (= {:continents {:name :continents}
            :countries {:name :countries}}))))

(deftest test-create-schema
  (with-redefs [jdbc/do-commands #(is (= "CREATE SCHEMA public" %1))]
    (create-schema (make-schema :public)))
  (with-quoted-identifiers \"
    (with-redefs [jdbc/do-commands #(is (= "CREATE SCHEMA \"public\"" %1))]
      (create-schema (make-schema :public)))))

(deftest test-as-identifier
  (is (= "public" (as-identifier (make-schema :public))))
  (with-quoted-identifiers \"
    (is (= "\"public\"" (as-identifier (make-schema :public))))))