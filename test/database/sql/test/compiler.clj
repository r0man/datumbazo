(ns database.sql.test.compiler
  (:use clojure.test
        database.sql
        database.sql.compiler))

(deftest test-compile-sql
  (are [expr expected]
       (is (= expected (compile-sql (parse-expr expr))))
       nil
       ["NULL"]
       1
       ["1"]
       1.2
       ["1.2"]
       "Europe"
       ["?" "Europe"]
       :continents.id
       ["continents.id"]
       '(greatest 1 2)
       ["greatest(1, 2)"]
       '(lower "Europe")
       ["lower(?)" "Europe"]
       '(upper (lower "Europe"))
       ["upper(lower(?))" "Europe"]
       '(ST_AsText (ST_Centroid "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"))
       ["ST_AsText(ST_Centroid(?))" "MULTIPOINT(-1 0, -1 2, -1 3, -1 4, -1 7, 0 1, 0 3, 1 1, 2 0, 6 0, 7 8, 9 8, 10 6)"]))
