(ns datumbazo.connection-test
  (:import [com.jolbox.bonecp BoneCPDataSource ConnectionHandle]
           com.mchange.v2.c3p0.ComboPooledDataSource
           com.mchange.v2.c3p0.impl.NewProxyConnection
           java.sql.Connection)
  (:require [clojure.java.jdbc :as jdbc]
            [com.stuartsierra.component :refer [start stop]]
            [environ.core :refer [env]]
            [datumbazo.core :refer [select]])
  (:use datumbazo.connection
        datumbazo.util
        datumbazo.test
        clojure.test))

(def test-url (connection-url :test-db))

(deftest test-connection-spec
  (let [spec (connection-spec "mysql://tiger:scotch@localhost/datumbazo?profileSQL=true")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "com.mysql.jdbc.Driver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
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
    (is (= "scotch" (:password spec))))
  (let [spec (connection-spec "postgresql://tiger:scotch@localhost:5432/datumbazo?ssl=true")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "org.postgresql.Driver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
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
    (is (= "scotch" (:password spec))))
  (let [spec (connection-spec "sqlite://tmp/datumbazo.sqlite")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "org.sqlite.JDBC" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= {} (:params spec)))
    (is (= "sqlite" (:subprotocol spec)))
    (is (= "//tmp/datumbazo.sqlite" (:subname spec))))
  (let [spec (connection-spec "sqlite:datumbazo.sqlite")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "org.sqlite.JDBC" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= {} (:params spec)))
    (is (= "sqlite" (:subprotocol spec)))
    (is (= "datumbazo.sqlite" (:subname spec))))
  (let [spec (connection-spec "sqlserver://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "com.microsoft.sqlserver.jdbc.SQLServerDriver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {} (:params spec)))
    (is (= "sqlserver" (:subprotocol spec)))
    (is (= "//localhost;database=datumbazo;user=tiger;password=scotch" (:subname spec))))
  (let [spec (connection-spec "oracle://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "oracle.jdbc.driver.OracleDriver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {} (:params spec)))
    (is (= "oracle:thin" (:subprotocol spec)))
    (is (= ":tiger/scotch@localhost:datumbazo" (:subname spec))))
  (let [spec (connection-spec "vertica://tiger:scotch@localhost/datumbazo")]
    (is (instance? sqlingvo.db.Database spec))
    (is (= "vertica" (:subprotocol spec)))
    (is (= "com.vertica.jdbc.Driver" (:classname spec)))
    (is (= :jdbc (:db-pool spec)))
    (is (= "localhost" (:host spec)))
    (is (nil? (:port spec)))
    (is (= "tiger" (:username spec)))
    (is (= "scotch" (:password spec)))
    (is (= "datumbazo" (:name spec)))
    (is (= "/datumbazo" (:uri spec)))
    (is (= {} (:params spec)))
    (is (= "vertica" (:subprotocol spec)))
    (is (= "//localhost/datumbazo" (:subname spec)))))

(deftest test-connection-url
  (with-test-db [db]
    (is (thrown? IllegalArgumentException (connection-url :unknown-db))))
  (is (= "postgresql://tiger:scotch@localhost/datumbazo" (connection-url :test-db))))

(deftest test-connection
  (with-test-db [db]
    (let [connection (connection test-url)]
      (is (map? connection)))))

(deftest test-cached-connection
  (with-test-db [db]
    (let [connection (cached-connection test-url)]
      (is (= connection (cached-connection test-url))))))

(deftest test-query
  (with-test-db [db]
    (is (= [[1]] (map vals (jdbc/query db ["SELECT 1"]))))))

(deftest test-jdbc-url
  (is (= "jdbc:postgresql://localhost/datumbazo?password=scotch&user=tiger"
         (jdbc-url (connection-spec (connection-url :test-db))))))

(deftest test-with-connection
  (with-connection [connection (env :test-db)]
    (is (map? connection))
    (is (= 0 (:level connection)))
    (is (instance? java.sql.Connection (:connection connection)))
    (is (= (env :test-db) (:connection-string connection)))))

(deftest test-start
  (let [db (connection-spec test-url)
        db (start db)]
    (is (instance? java.sql.Connection (:connection db)))
    (.close (:connection db))))

(deftest test-start
  (let [db (connection-spec test-url)
        db (stop (start db))]
    (is (nil? (:connection db)))))

(deftest test-lifecycle
  (let [component (connection-spec test-url)
        started (start component)]
    (is (:connection started))
    (is (thrown? clojure.lang.ExceptionInfo (start started)))
    (let [stopped (stop started)]
      (is (nil? (:connection stopped)))
      (is (map? (stop stopped))))))

(deftest test-with-db
  (with-db [db (connection-spec test-url)]
    (is (instance? java.sql.Connection (:connection db)))))

(deftest test-with-db-rollback
  (let [component (connection-spec test-url)
        component (assoc component :test true)]
    (with-db [db component]
      (is (instance? java.sql.Connection (:connection db))))))

(deftest test-sql-str
  (with-test-db [db]
    (is (= "SELECT 1, 'a'" (sql-str db (select [1 "a"]))))))
