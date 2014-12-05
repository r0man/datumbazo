(ns datumbazo.db-test
  (:require [datumbazo.db :refer :all]
            [clojure.test :refer :all]))

(deftest test-new-db-bonecp
  (let [url "postgresql://localhost/datumbazo"]
    (is (= (new-db (str "bonecp:" url))
           (assoc (new-db url) :pool :bonecp)))))

(deftest test-new-db-c3p0
  (let [url "postgresql://localhost/datumbazo"]
    (is (= (new-db (str "c3p0:" url))
           (assoc (new-db url) :pool :c3p0)))))

(deftest test-new-db-mysql
  (let [spec (new-db "mysql://tiger:scotch@localhost/datumbazo?profileSQL=true")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "com.mysql.jdbc.Driver" (:classname spec)))
    (is (= :jdbc (:pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {:profileSQL "true"} (:params spec)))
    (is (= "mysql" (:subprotocol spec)))
    (is (= "//localhost/datumbazo?profileSQL=true" (:subname spec)))
    (is (= "tiger" (:user spec))) ; MySQL needs :user key
    (is (= "scotch" (:password spec)))))

(deftest test-new-db-oracle
  (let [spec (new-db "oracle://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "oracle.jdbc.driver.OracleDriver" (:classname spec)))
    (is (= :jdbc (:pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (nil? (:params spec)))
    (is (= "oracle:thin" (:subprotocol spec)))
    (is (= ":tiger/scotch@localhost:datumbazo" (:subname spec)))))

(deftest test-new-db-postgresql
  (let [spec (new-db "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "org.postgresql.Driver" (:classname spec)))
    (is (= :jdbc (:pool spec)))
    (is (= "localhost" (:host spec)))
    (is (= 5432 (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {:ssl "true"} (:params spec)))
    (is (= "postgresql" (:subprotocol spec)))
    (is (= "//localhost:5432/datumbazo?ssl=true" (:subname spec)))
    (is (= "tiger" (:user spec)))
    (is (= "scotch" (:password spec)))))

(deftest test-new-db-sqlite
  (let [spec (new-db "sqlite://tmp/datumbazo.sqlite")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "org.sqlite.JDBC" (:classname spec)))
    (is (= :jdbc (:pool spec)))
    (is (nil? (:params spec)))
    (is (= "sqlite" (:subprotocol spec)))
    (is (= "//tmp/datumbazo.sqlite" (:subname spec)))))

(deftest test-new-db-sqlserver
  (let [spec (new-db "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "com.microsoft.sqlserver.jdbc.SQLServerDriver" (:classname spec)))
    (is (= :jdbc (:pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (nil? (:params spec)))
    (is (= "sqlserver" (:subprotocol spec)))
    (is (= "//localhost;database=datumbazo;user=tiger;password=scotch" (:subname spec)))))

(deftest test-new-db-vertica
  (let [spec (new-db "vertica://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "vertica" (:subprotocol spec)))
    (is (= "com.vertica.jdbc.Driver" (:classname spec)))
    (is (= :jdbc (:pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (nil? (:params spec)))
    (is (= "vertica" (:subprotocol spec)))
    (is (= "//localhost/datumbazo" (:subname spec)))))

(deftest test-new-db-with-db
  (let [url "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true"]
    (is (= (new-db (new-db url))
           (new-db url)))))

(deftest test-parse-url
  (doseq [url [nil "" "x"]]
    (is (thrown? clojure.lang.ExceptionInfo (parse-url url))))
  (let [spec (parse-url "postgresql://localhost:5432/datumbazo")]
    (is (= :jdbc (:pool spec)))
    (is (= "localhost" (:host spec)))
    (is (= 5432 (:port spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (nil? (:params spec)))
    (is (= "postgresql" (:subprotocol spec))))
  (let [spec (parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2")]
    (is (= :jdbc (:pool spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "localhost" (:host spec)))
    (is (= 5432 (:port spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {:a "1" :b "2"} (:params spec)))
    (is (= "postgresql" (:subprotocol spec))))
  (let [spec (parse-url "c3p0:postgresql://localhost/datumbazo")]
    (is (= :c3p0 (:pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (nil?  (:port spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (nil? (:params spec)))
    (is (= "postgresql" (:subprotocol spec)))))
