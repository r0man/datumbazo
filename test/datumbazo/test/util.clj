(ns datumbazo.test.util
  (:use clojure.test
        datumbazo.util))

(deftest test-absolute-path
  (is (string? (absolute-path ""))))

(deftest test-clojure-file?
  (is (not (clojure-file? "NOT-EXISTING")))
  (is (not (clojure-file? "src")))
  (is (clojure-file? "project.clj")))

(deftest test-clojure-file-seq
  (let [files (clojure-file-seq "src")]
    (is (not (empty? files)))
    (is (every? clojure-file? files))))

(deftest test-path-split
  (is (= [""] (path-split nil)))
  (is (= [""] (path-split "")))
  (is (= ["x" "y"] (path-split "x/y"))))

(deftest test-path-replace
  (is (= "database" (path-replace "src/database" "src"))))

(deftest test-parse-integer
  (is (nil? (parse-integer nil)))
  (is (nil? (parse-integer "")))
  (is (= 1 (parse-integer 1)))
  (is (= 1 (parse-integer "1"))))

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

(deftest test-parse-column
  (are [table expected]
       (do (is (= expected (parse-column table)))
           (is (= expected (parse-column table))))
       :id
       {:op :column, :schema nil, :table nil, :name :id, :as nil}
       :continents.id
       {:op :column, :schema nil, :table :continents, :name :id, :as nil}
       :continents.id/i
       {:op :column, :schema nil, :table :continents, :name :id, :as :i}
       :public.continents.id
       {:op :column, :schema :public, :table :continents, :name :id, :as nil}
       :public.continents.id/i
       {:op :column, :schema :public, :table :continents, :name :id, :as :i}))

(deftest test-parse-table
  (are [table expected]
       (do (is (= expected (parse-table table)))
           (is (= expected (parse-table (qualified-name table)))))
       :continents
       {:op :table :schema nil :name :continents :as nil}
       :continents/c
       {:op :table :schema nil :name :continents :as :c}
       :public.continents
       {:op :table :schema :public :name :continents :as nil}
       :public.continents/c
       {:op :table :schema :public :name :continents :as :c}))

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