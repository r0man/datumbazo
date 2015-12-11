(ns datumbazo.driver.funcool
  (:require [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [datumbazo.driver.core :refer :all]
            [sqlingvo.compiler :refer [compile-stmt]])
  (:import sqlingvo.db.Database))

(defmethod apply-transaction 'jdbc.core [db f & [opts]]
  (jdbc/atomic-apply
   (:connection db)
   (fn [connection]
     (f (assoc db :connection connection)))
   opts))

(defmethod close-db 'jdbc.core [db]
  (.close (:connection db)))

(defmethod eval-db* 'jdbc.core
  [{:keys [db] :as ast} & [opts]]
  (let [conn (:connection db)
        sql (compile-stmt db ast)]
    (assert conn "No database connection!")
    (case (:op ast)
      :delete
      (if (:returning ast)
        (jdbc/fetch conn sql)
        (row-count (jdbc/execute conn sql)))
      :except
      (jdbc/fetch conn sql)
      :insert
      (if (:returning ast)
        (jdbc/fetch conn sql)
        (row-count (jdbc/execute conn sql)))
      :intersect
      (jdbc/fetch conn sql)
      :select
      (jdbc/fetch conn sql)
      :union
      (jdbc/fetch conn sql)
      :update
      (if (:returning ast)
        (jdbc/fetch conn sql)
        (row-count (jdbc/execute conn sql)))
      (row-count (jdbc/execute conn sql)))))

(defmethod open-db 'jdbc.core [db]
  (assoc db :connection (jdbc/connection (into {} db))))
