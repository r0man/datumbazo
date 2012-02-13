(ns database.test.util
  (:use clojure.test
        database.util))

(deftest test-parse-integer
  (testing "junk allowed"
    (are [string]
      (is (nil? (parse-integer string :junk-allowed true)))
      nil "" "junk"))
  (testing "junk not allwed"
    (are [junk]
      (is (thrown? Exception (parse-integer junk)))
      nil "" "junk"))
  (testing "valid integer"
    (are [string expected]
      (is (= (parse-integer string) expected))
      "-1" -1
      "0" 0
      "25" 25
      "12351" 12351
      "321-mundaka" 321
      2344 2344)))