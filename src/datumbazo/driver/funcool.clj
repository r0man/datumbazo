(ns datumbazo.driver.funcool
  (:require [jdbc.core :as jdbc]
            [datumbazo.driver.core :refer :all]
            [sqlingvo.compiler :refer [compile-stmt]]))

(defmethod close-db 'jdbc.core [db]
  (.close (:connection db)))

(defmethod eval-db* 'jdbc.core
  [{:keys [db] :as ast}]
  (let [sql (compile-stmt db ast)]
    (case (:op ast)
      :delete
      (if (:returning ast)
        (jdbc/fetch db sql)
        (row-count (jdbc/execute db sql)))
      :except
      (jdbc/fetch db sql)
      :insert
      (if (:returning ast)
        (jdbc/fetch db sql)
        (row-count (jdbc/execute db sql)))
      :intersect
      (jdbc/fetch db sql)
      :select
      (jdbc/fetch db sql)
      :union
      (jdbc/fetch db sql)
      :update
      (if (:returning ast)
        (jdbc/fetch db sql)
        (row-count (jdbc/execute db sql)))
      (row-count (jdbc/execute db sql)))))

(defmethod open-db 'jdbc.core [db]
  (assoc db :connection (jdbc/connection db)))
