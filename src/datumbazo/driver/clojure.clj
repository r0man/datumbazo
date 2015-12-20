(ns datumbazo.driver.clojure
  (:require [clojure.java.jdbc :as jdbc]
            [datumbazo.driver.core :refer :all]
            [sqlingvo.compiler :refer [compile-stmt]]))

(defmethod apply-transaction 'clojure.java.jdbc [db f & [opts]]
  (jdbc/db-transaction* db f))

(defmethod close-db 'clojure.java.jdbc [db]
  (.close (:connection db)))

(defmethod eval-db* 'clojure.java.jdbc
  [{:keys [db] :as ast} & [opts]]
  (let [sql (compile-stmt ast)]
    (case (:op ast)
      :except
      (jdbc/query db sql)
      :delete
      (if (:returning ast)
        (jdbc/query db sql)
        (row-count (jdbc/execute! db sql)))
      :insert
      (if (:returning ast)
        (jdbc/query db sql)
        (row-count (jdbc/execute! db sql)))
      :intersect
      (jdbc/query db sql)
      :select
      (jdbc/query db sql)
      :union
      (jdbc/query db sql)
      :update
      (if (:returning ast)
        (jdbc/query db sql)
        (row-count (jdbc/execute! db sql)))
      (row-count (jdbc/execute! db sql)))))

(defmethod open-db 'clojure.java.jdbc [db]
  (assoc db :connection (jdbc/get-connection db)))
