(ns datumbazo.db-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [datumbazo.db :refer :all]
            [datumbazo.test :refer :all])
  (:import com.jolbox.bonecp.BoneCPDataSource
           com.mchange.v2.c3p0.ComboPooledDataSource
           com.zaxxer.hikari.HikariDataSource
           javax.sql.DataSource
           java.sql.Connection))

(deftest test-new-db-mysql
  (let [db (new-db "mysql://tiger:scotch@localhost/datumbazo?profileSQL=true")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "com.mysql.jdbc.Driver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:profileSQL "true"} (:query-params db)))
    (is (= "mysql" (:subprotocol db)))
    (is (= :mysql (:scheme db)))
    (is (= "tiger" (:user db))) ; MySQL needs :user key
    (is (= "scotch" (:password db)))))

(deftest test-new-db-oracle
  (let [db (new-db "oracle://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "oracle.jdbc.driver.OracleDriver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= "oracle" (:subprotocol db)))
    (is (= :oracle (:scheme db)))))

(deftest test-new-db-postgresql
  (let [db (new-db "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (nil? (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:ssl "true"} (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))))

(deftest test-new-db-postgresql-c3p0
  (let [db (new-db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :c3p0 (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (empty? (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (let [started (component/start db)]
      (is (instance? ComboPooledDataSource (:datasource started)))
      (is (instance? Connection (.getConnection (:datasource started))))
      (component/stop started))))

(deftest test-new-db-bonecp
  (let [db (new-db "bonecp:postgresql://tiger:scotch@localhost:5432/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :bonecp (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (empty? (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (let [started (component/start db)]
      (is (instance? BoneCPDataSource (:datasource started)))
      (is (instance? Connection (.getConnection (:datasource started))))
      (component/stop started))))

(deftest test-new-db-hikaricp
  (let [db (new-db "hikaricp:postgresql://tiger:scotch@localhost:5432/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:datasource db)))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :hikaricp (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (empty? (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (let [started (component/start db)]
      (is (instance? HikariDataSource (:datasource started)))
      (is (instance? Connection (.getConnection (:datasource started))))
      (component/stop started))))

(deftest test-new-db-sqlite
  (let [db (new-db "sqlite://tmp/datumbazo.sqlite")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "org.sqlite.JDBC" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:query-params db)))
    (is (= "sqlite" (:subprotocol db)))
    (is (= :sqlite (:scheme db)))))

(deftest test-new-db-sqlserver
  (let [db (new-db "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "com.microsoft.sqlserver.jdbc.SQLServerDriver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= "sqlserver" (:subprotocol db)))))

(deftest test-new-db-vertica
  (let [db (new-db "vertica://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "vertica" (:subprotocol db)))
    (is (= "com.vertica.jdbc.Driver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= "vertica" (:subprotocol db)))
    (is (= :vertica (:scheme db)))))

(deftest test-new-db-with-db
  (let [url "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true"]
    (is (= (new-db (new-db url))
           (new-db url)))))

(deftest test-parse-url
  (doseq [url [nil "" "x"]]
    (is (thrown? clojure.lang.ExceptionInfo (parse-url url))))
  (let [db (parse-url "mysql://localhost:5432/datumbazo")]
    (is (nil? (:pool db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= :mysql (:scheme db))))
  (let [db (parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2")]
    (is (nil? (:pool db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:server-name db)))
    (is (= 5432 (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:a "1" :b "2"} (:query-params db)))
    (is (= :postgresql (:scheme db))))
  (let [db (parse-url "c3p0:postgresql://localhost/datumbazo")]
    (is (= :c3p0 (:pool db)))
    (is (= "localhost" (:server-name db)))
    (is (nil? (:server-port db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= :postgresql (:scheme db)))))

(deftest test-format-url
  (let [url "postgresql://tiger:scotch@localhost/datumbazo?a=1&b=2"]
    (is (= (format-url (parse-url url)) url)))
  (let [url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2"]
    (is (= (format-url (parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2"))
           "postgresql://tiger:scotch@localhost/datumbazo?a=1&b=2"))))

(deftest test-with-db
  (with-db [db (:postgresql connections)]
    (is (nil? (:connection db))))
  (with-db [db (:postgresql connections) {:rollback? true}]
    (is (instance? Connection (:test-connection db))))
  (doseq [pool [:bonecp :c3p0 :hikaricp]]
    (with-db [db (:postgresql connections) {:rollback? true :pool pool}]
      (is (instance? DataSource (:datasource db)))
      (is (instance? Connection (:test-connection db))))))
