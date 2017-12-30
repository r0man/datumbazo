(ns datumbazo.db-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all])
  (:import [com.jolbox.bonecp BoneCPDataSource ConnectionHandle]
           com.mchange.v2.c3p0.ComboPooledDataSource
           com.mchange.v2.c3p0.impl.NewProxyConnection
           com.zaxxer.hikari.HikariDataSource
           com.zaxxer.hikari.pool.HikariProxyConnection
           java.sql.Connection
           javax.sql.DataSource))

(deftest test-new-db-mysql
  (let [db (sql/new-db "mysql://tiger:scotch@localhost/datumbazo?profileSQL=true")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "com.mysql.cj.jdbc.Driver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:profileSQL "true"} (:query-params db)))
    (is (= :mysql (:scheme db)))
    (is (= "tiger" (:username db))) ; MySQL needs :user key
    (is (= "scotch" (:password db)))))

(deftest test-new-db-oracle
  (let [db (sql/new-db "oracle://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "oracle.jdbc.driver.OracleDriver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= :oracle (:scheme db)))))

(deftest test-new-db-postgresql
  (let [db (sql/new-db "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (nil? (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= {:ssl "true"} (:query-params db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))))

(deftest test-new-db-postgresql-c3p0
  (let [db (sql/new-db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :c3p0 (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (empty? (:query-params db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (let [started (component/start db)]
      (is (instance? ComboPooledDataSource (:datasource started)))
      (sql/with-connection [db started]
        (is (instance? NewProxyConnection (sql/connection db))))
      (component/stop started))))

(deftest test-new-db-bonecp
  (let [db (sql/new-db "bonecp:postgresql://tiger:scotch@localhost:5432/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :bonecp (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (empty? (:query-params db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (let [started (component/start db)]
      (is (instance? BoneCPDataSource (:datasource started)))
      (sql/with-connection [db started]
        (is (instance? ConnectionHandle (sql/connection db))))
      (component/stop started))))

(deftest test-new-db-hikaricp
  (let [db (sql/new-db "hikaricp:postgresql://tiger:scotch@localhost:5432/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:datasource db)))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :hikaricp (:pool db)))
    (is (= 5432 (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (empty? (:query-params db)))
    (is (= :postgresql (:scheme db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (let [started (component/start db)]
      (is (instance? HikariDataSource (:datasource started)))
      (sql/with-connection [db started]
        (is (instance? HikariProxyConnection (sql/connection db))))
      (component/stop started))))

(deftest test-new-db-sqlite
  (let [db (sql/new-db "sqlite://tmp/datumbazo.sqlite")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "org.sqlite.JDBC" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:query-params db)))
    (is (= :sqlite (:scheme db)))))

(deftest test-new-db-sqlserver
  (let [db (sql/new-db "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "com.microsoft.sqlserver.jdbc.SQLServerDriver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))))

(deftest test-new-db-vertica
  (let [db (sql/new-db "vertica://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "com.vertica.jdbc.Driver" (:classname db)))
    (is (nil? (:pool db)))
    (is (nil? (:server-port db)))
    (is (= "tiger" (:username db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (nil? (:query-params db)))
    (is (= :vertica (:scheme db)))))

(deftest test-with-db
  (sql/with-db [db (:postgresql connections)]
    (is (:driver db))))

(deftest test-with-db-pool
  (doseq [pool [:bonecp :c3p0 :hikaricp]]
    (sql/with-db [db (:postgresql connections) {:pool pool}]
      (is (:driver db))
      (is (instance? DataSource (:datasource db))))))
