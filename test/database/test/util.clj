(ns database.test.util
  (:use clojure.test
        database.util))

(deftest test-parse-int
  (are [serial expected]
    (is (= expected (parse-int serial)))
    nil nil
    "" nil
    "nil" nil
    "1.0" nil ; TODO: Really?
    "1" 1
    (str Integer/MIN_VALUE) Integer/MIN_VALUE
    (str Integer/MAX_VALUE) Integer/MAX_VALUE))
