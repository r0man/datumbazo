(ns datumbazo.db.postgresql.error-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.core :as sql]
            [datumbazo.test :refer :all]))

(deftest test-execute-error
  (with-backends [db]
    (try @(sql/select db [:*]
            (sql/from :unknown))
         (catch Exception e
           (is (= {:type :datumbazo/error
                   :datumbazo.error/code 0,
                   :datumbazo.error/message "relation \"unknown\" does not exist",
                   :datumbazo.error/state "42P01",
                   :datumbazo.error/type :datumbazo.postgresql.error/undefined-table,
                   :datumbazo.error/sql ["SELECT * FROM \"unknown\""]}
                  (ex-data e)))))))
