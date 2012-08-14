(ns database.test.schema
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.connection
        database.schema
        database.protocol))

(deftest test-make-schema
  (let [tables [{:name :continents} {:name :countries}]]
    (is (thrown? AssertionError (make-schema nil)))
    (is (= (make-schema :public) (make-schema "public")))
    (let [schema (make-schema :public tables)]
      (is (= :public (:name schema)))
      (is (= tables (:tables schema))))))

(deftest test-drop-schema
  (with-redefs [jdbc/do-commands #(is (= "DROP SCHEMA public" %1))]
    (drop-schema (make-schema :public)))
  (with-quoted-identifiers \"
    (with-redefs [jdbc/do-commands #(is (= "DROP SCHEMA \"public\"" %1))]
      (drop-schema (make-schema :public)))))

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
