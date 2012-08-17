(ns database.test.ddl
  (:require [clojure.java.jdbc :as jdbc])
  (:use clojure.test
        database.connection
        database.ddl
        database.meta))

(deftest test-create-schema
  (with-redefs [jdbc/do-commands #(is (= "CREATE SCHEMA public" %1))]
    (create-schema (make-schema :public)))
  (with-quoted-identifiers \"
    (with-redefs [jdbc/do-commands #(is (= "CREATE SCHEMA \"public\"" %1))]
      (create-schema (make-schema :public)))))

(deftest test-drop-schema
  (with-redefs [jdbc/do-commands #(is (= "DROP SCHEMA public" %1))]
    (drop-schema (make-schema :public)))
  (with-quoted-identifiers \"
    (with-redefs [jdbc/do-commands #(is (= "DROP SCHEMA \"public\"" %1))]
      (drop-schema (make-schema :public)))))

(deftest test-drop-table
  (with-redefs [jdbc/do-commands #(is (= "DROP TABLE oauth.applications" %1))]
    (drop-table (make-table :oauth.applications)))
  (with-quoted-identifiers \"
    (with-redefs [jdbc/do-commands #(is (= "DROP TABLE \"oauth\".\"applications\"" %1))]
      (drop-table (make-table :oauth.applications)))))
