(ns datumbazo.driver
  (:require [datumbazo.driver.core :refer [eval-db]]))

(defn make-db [db-spec]
  (let [db (sqlingvo.db/postgresql db-spec)]
    (assoc db :eval-fn eval-db)))

(try
  (doseq [ns '[datumbazo.driver.clojure
               datumbazo.driver.funcool]]
    (try (require ns)
         (catch Exception _))))
