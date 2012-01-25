(ns database.test.util
  (:use clojure.test
        database.util
        database.test))

;; (database-test test-make-sql-array
;;   (let [array (make-sql-array "int" [1 2])]
;;     (is (instance? java.sql.Array array))
;;     (let [rs (resultset-seq (.getResultSet array))]
;;       (is (= {:index 1 :value 1} (first rs)))
;;       (is (= {:index 2 :value 2} (second rs))))))

;; (database-test test-make-text-array
;;   (let [array (make-text-array ["1" "2"])]
;;     (is (instance? java.sql.Array array))
;;     (let [rs (resultset-seq (.getResultSet array))]
;;       (is (= {:index 1 :value "1"} (first rs)))
;;       (is (= {:index 2 :value "2"} (second rs))))))

;; (database-test test-sql-array-seq
;;   (is (= ["1" "2"] (sql-array-seq (make-text-array ["1" "2"])))))

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
