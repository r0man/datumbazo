(ns datumbazo.validation-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer [run1]]
            [datumbazo.test :refer [with-test-db]]
            [datumbazo.validation :refer :all]
            [sqlingvo.core :refer [sql]]
            [validation.core :refer :all]))

(deftest test-uniqueness-of
  (with-test-db [db]
    (with-redefs [run1 (fn [stmt]
                         (is (= ["SELECT \"nick\" FROM \"users\" WHERE (\"nick\" = ?) LIMIT 1" "Bob"]
                                (sql stmt)))
                         [])]
      (let [errors (:errors (meta ((uniqueness-of db :users :nick) {:nick "Bob"})))]
        (is (= ["has already been taken"] (:nick errors)))))))
