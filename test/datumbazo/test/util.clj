(ns datumbazo.test.util
  (:refer-clojure :exclude [distinct group-by])
  (:use clojure.test
        datumbazo.util
        datumbazo.core
        datumbazo.test))

(deftest test-absolute-path
  (is (string? (absolute-path ""))))

(deftest test-edn-file?
  (is (not (edn-file? "NOT-EXISTING")))
  (is (not (edn-file? "src")))
  (is (edn-file? "test-resources/db/test-db/fixtures/continents.edn")))

(deftest test-edn-file-seq
  (let [files (edn-file-seq "test-resources/db/test-db/fixtures")]
    (is (not (empty? files)))
    (is (every? edn-file? files))))

(deftest test-format-server
  (are [server expected]
       (is (= expected (format-server server)))
       {:server-name "example.com"}
       "example.com"
       {:server-name "example.com" :server-port 123}
       "example.com:123"))

(deftest test-path-split
  (is (= [""] (path-split nil)))
  (is (= [""] (path-split "")))
  (is (= ["x" "y"] (path-split "x/y"))))

(deftest test-path-replace
  (is (= "database" (path-replace "src/database" "src"))))

(deftest test-parse-params
  (are [params expected]
       (is (= expected (parse-params params)))
       nil {}
       "" {}
       "a=1" {:a "1"}
       "a=1&b=2" {:a "1" :b "2"}))

(deftest test-parse-schema
  (are [schema expected]
       (do (is (= expected (parse-schema schema)))
           (is (= expected (parse-schema (qualified-name schema)))))
       :public {:name :public}))

(deftest test-parse-url
  (doseq [url [nil "" "x"]] (is (nil? (parse-url nil))))
  (let [spec (parse-url "postgresql://localhost:5432/lein_test")]
    (is (= "postgresql" (:scheme spec)))
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "localhost" (:server-name spec)))
    (is (= "localhost" (:host spec)))
    (is (= 5432 (:server-port spec)))
    (is (= 5432 (:port spec)))
    (is (= "lein_test" (:db spec)))
    (is (= "/lein_test" (:uri spec)))
    (is (= "//localhost:5432/lein_test" (:subname spec)))
    (is (= {} (:params spec))))
  (let [spec (parse-url "postgresql://tiger:scotch@localhost:5432/lein_test?a=1&b=2")]
    (is (= "postgresql" (:scheme spec)))
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "tiger" (:user spec)))
    (is (= "scotch" (:password spec)))
    (is (= "localhost" (:server-name spec)))
    (is (= "localhost" (:host spec)))
    (is (= 5432 (:server-port spec)))
    (is (= 5432 (:port spec)))
    (is (= "lein_test" (:db spec)))
    (is (= "/lein_test" (:uri spec)))
    (is (= "//localhost:5432/lein_test" (:subname spec)))
    (is (= {:a "1" :b "2"} (:params spec))))
  (let [spec (parse-url "jdbc:postgresql://localhost/lein_test")]
    (is (= "postgresql" (:scheme spec)))
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "localhost" (:server-name spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:server-port spec)))
    (is (nil?  (:port spec)))
    (is (= "lein_test" (:db spec)))
    (is (= "/lein_test" (:uri spec)))
    (is (= "//localhost/lein_test" (:subname spec)))
    (is (= {} (:params spec)))))

(deftest test-qualified-name
  (are [arg expected]
       (is (= expected (qualified-name arg)))
       nil ""
       "" ""
       "continents" "continents"
       :continents "continents"
       :public.continents "public.continents"))

(deftest test-parse-subprotocol
  (doseq [db-url [nil "" "x"]]
    (is (thrown? IllegalArgumentException (parse-subprotocol db-url))))
  (are [db-url subprotocol]
       (is (= subprotocol (parse-subprotocol db-url)))
       "bonecp:mysql://localhost/korma" "mysql"
       "c3p0:mysql://localhost/korma" "mysql"
       "jdbc:mysql://localhost/korma" "mysql"
       "mysql://localhost/korma" "mysql"))

(deftest test-parse-db-url
  (doseq [url [nil "" "x"]]
    (is (thrown? IllegalArgumentException (parse-db-url url))))
  (let [url (parse-db-url "postgresql://localhost:5432/korma")]
    (is (= :jdbc (:pool url)))
    (is (= "localhost" (:server-name url)))
    (is (= 5432 (:server-port url)))
    (is (= "korma" (:db url)))
    (is (= "/korma" (:uri url)))
    (is (= {} (:params url)))
    (let [spec (:spec url)]
      (is (= "//localhost:5432/korma" (:subname spec)))
      (is (= "postgresql" (:subprotocol spec)))))
  (let [url (parse-db-url "postgresql://tiger:scotch@localhost:5432/korma?a=1&b=2")]
    (is (= :jdbc (:pool url)))
    (is (= "tiger" (:user url)))
    (is (= "scotch" (:password url)))
    (is (= "localhost" (:server-name url)))
    (is (= 5432 (:server-port url)))
    (is (= "korma" (:db url)))
    (is (= "/korma" (:uri url)))
    (is (= {:a "1" :b "2"} (:params url)))
    (let [spec (:spec url)]
      (is (= "//localhost:5432/korma?a=1&b=2" (:subname spec)))
      (is (= "postgresql" (:subprotocol spec)))))
  (let [url (parse-db-url "c3p0:postgresql://localhost/korma")]
    (is (= :c3p0 (:pool url)))
    (is (= "localhost" (:server-name url)))
    (is (nil? (:server-port url)))
    (is (nil?  (:port url)))
    (is (= "korma" (:db url)))
    (is (= "/korma" (:uri url)))
    (is (= {} (:params url)))
    (let [spec (:spec url)]
      (is (= "//localhost/korma" (:subname spec)))
      (is (= "postgresql" (:subprotocol spec))))))

(deftest test-slurp-sql
  (let [stmts (slurp-sql "test-resources/stmts-simple.sql")]
    (is (= ["DROP TABLE x" "DROP TABLE y"] stmts)))
  (let [stmts (slurp-sql "test-resources/stmts-raster.sql")]
    (is (= 2 (count stmts)))))

(database-test test-exec-sql-file
  (run db (drop-table [:akw-dirpwsfc-2013-02-10t06]
             (if-exists true)))
  (exec-sql-file db "test-resources/stmts-raster.sql"))
