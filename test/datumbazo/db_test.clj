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
    (is (= :jdbc (:pool db)))
    (is (= "localhost" (:host db)))
    (is (nil? (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (= {:profileSQL "true"} (:query-params db)))
    (is (= "mysql" (:subprotocol db)))
    (is (= :mysql (:scheme db)))
    (is (= "//localhost/datumbazo?profileSQL=true" (:subname db)))
    (is (= "tiger" (:user db))) ; MySQL needs :user key
    (is (= "scotch" (:password db)))))

(deftest test-new-db-oracle
  (let [db (new-db "oracle://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "oracle.jdbc.driver.OracleDriver" (:classname db)))
    (is (= :jdbc (:pool db)))
    (is (= "localhost" (:host db)))
    (is (nil? (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (nil? (:query-params db)))
    (is (= "oracle" (:subprotocol db)))
    (is (= :oracle (:scheme db)))
    (is (= ":tiger/scotch@localhost:datumbazo" (:subname db)))))

(deftest test-new-db-postgresql
  (let [db (new-db "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :jdbc (:pool db)))
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (= {:ssl "true"} (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "//localhost:5432/datumbazo?ssl=true" (:subname db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))))

(deftest test-new-db-postgresql-c3p0
  (let [db (new-db "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (= "org.postgresql.Driver" (:classname db)))
    (is (= :c3p0 (:pool db)))
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (empty? (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "//localhost:5432/datumbazo" (:subname db)))
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
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (empty? (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "//localhost:5432/datumbazo" (:subname db)))
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
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (empty? (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))
    (is (= :postgresql (:scheme db)))
    (is (= "//localhost:5432/datumbazo" (:subname db)))
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
    (is (= :jdbc (:pool db)))
    (is (nil? (:query-params db)))
    (is (= "sqlite" (:subprotocol db)))
    (is (= :sqlite (:scheme db)))
    (is (= "//tmp/datumbazo.sqlite" (:subname db)))))

(deftest test-new-db-sqlserver
  (let [db (new-db "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "com.microsoft.sqlserver.jdbc.SQLServerDriver" (:classname db)))
    (is (= :jdbc (:pool db)))
    (is (= "localhost" (:host db)))
    (is (nil? (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (nil? (:query-params db)))
    (is (= "sqlserver" (:subprotocol db)))
    (is (= "//localhost;database=datumbazo;user=tiger;password=scotch" (:subname db)))))

(deftest test-new-db-vertica
  (let [db (new-db "vertica://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database db))
    (is (nil? (:db-pool db)))
    (is (= "vertica" (:subprotocol db)))
    (is (= "com.vertica.jdbc.Driver" (:classname db)))
    (is (= :jdbc (:pool db)))
    (is (= "localhost" (:host db)))
    (is (nil? (:port db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (nil? (:query-params db)))
    (is (= "vertica" (:subprotocol db)))
    (is (= :vertica (:scheme db)))
    (is (= "//localhost/datumbazo" (:subname db)))))

(deftest test-new-db-with-db
  (let [url "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true"]
    (is (= (new-db (new-db url))
           (new-db url)))))

(deftest test-parse-url
  (doseq [url [nil "" "x"]]
    (is (thrown? clojure.lang.ExceptionInfo (parse-url url))))
  (let [db (parse-url "postgresql://localhost:5432/datumbazo")]
    (is (= :jdbc (:pool db)))
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (nil? (:query-params db)))
    (is (= "postgresql" (:subprotocol db))))
  (let [db (parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?a=1&b=2")]
    (is (= :jdbc (:pool db)))
    (is (= "tiger" (:user db)))
    (is (= "scotch" (:password db)))
    (is (= "localhost" (:host db)))
    (is (= 5432 (:port db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (= {:a "1" :b "2"} (:query-params db)))
    (is (= "postgresql" (:subprotocol db))))
  (let [db (parse-url "c3p0:postgresql://localhost/datumbazo")]
    (is (= :c3p0 (:pool db)))
    (is (= "localhost" (:host db)))
    (is (nil? (:port db)))
    (is (nil?  (:port db)))
    (is (= "datumbazo" (:name db)))
    (is (= "/datumbazo" (:uri db)))
    (is (nil? (:query-params db)))
    (is (= "postgresql" (:subprotocol db)))))

(deftest test-subname-mysql
  (let [db (parse-url "mysql://tiger:scotch@localhost/datumbazo?profileSQL=true")]
    (is (= "//localhost/datumbazo?profileSQL=true" (subname db)))))

(deftest test-subname-oracle
  (let [db (parse-url "oracle://tiger:scotch@localhost/datumbazo")]
    (is (= ":tiger/scotch@localhost:datumbazo" (subname db)))))

(deftest test-subname-postgresql
  (let [db (parse-url "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (= "//localhost:5432/datumbazo?ssl=true" (subname db)))))

(deftest test-subname-c3p0-postgresql
  (let [db (parse-url "c3p0:postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (= "//localhost:5432/datumbazo?ssl=true" (subname db)))))

(deftest test-subname-sqlite
  (let [db (parse-url "sqlite://tmp/datumbazo.sqlite")]
    (is (= "//tmp/datumbazo.sqlite" (subname db)))))

(deftest test-subname-sqlserver
  (let [db (parse-url "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (= "//localhost;database=datumbazo;user=tiger;password=scotch" (subname db)))))

(deftest test-subname-vertica
  (let [db (parse-url "vertica://tiger:scotch@localhost/datumbazo")]
    (is (= "//localhost/datumbazo" (subname db)))))

(deftest test-with-db
  (with-db [db (:postgresql connections)]
    (is (nil? (:connection db))))
  (with-db [db (:postgresql connections) {:rollback? true}]
    (is (instance? Connection (:connection db))))
  (doseq [pool [:bonecp :c3p0 :hikaricp]]
    (with-db [db (:postgresql connections) {:rollback? true :pool pool}]
      (is (instance? DataSource (:datasource db)))
      (is (instance? Connection (:connection db))))))
