(ns datumbazo.validation-test
  (:require [clojure.test :refer :all]
            [datumbazo.core :refer [run1]]
            [datumbazo.validation :refer :all]
            [slingshot.slingshot :refer [try+]]
            [sqlingvo.core :refer [sql]]
            [validation.core :refer :all]))

(deftest test-uniqueness-of
  (with-redefs [run1 (fn [db stmt]
                       (is (= ["SELECT \"nick\" FROM \"users\" WHERE (\"nick\" = ?) LIMIT 1" "Bob"]
                              (sql stmt)))
                       [])]
    (let [errors (:errors (meta ((uniqueness-of nil :users :nick) {:nick "Bob"})))]
      (is (= "has already been taken" (:nick errors))))))
