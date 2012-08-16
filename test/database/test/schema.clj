(ns database.test.schema
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.connection
        database.schema
        database.test
        database.protocol))

(deftest test-make-schema
  (is (thrown? AssertionError (make-schema nil)))
  (is (= (make-schema :public) (make-schema "public")))
  (let [schema (make-schema :public)]
    (is (= :public (:name schema)))))

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

(deftest test-schema-key
  (is (= [:public] (schema-key (make-schema :public)))))

(deftest test-register-schema
  (let [schema (make-schema :oauth)]
    (is (= schema (register-schema schema)))
    (is (= schema (get @*schemas* (:name schema))))))


;; (database-test test-load-schemas
;;   (is (load-schemas)))
