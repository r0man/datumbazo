(ns datumbazo.driver.test
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as d]))

(defrecord Driver [driver connected?]
  d/IConnection
  (-connect [driver opts]
    (assoc driver :connected? true))
  (-connection [driver]
    (when connected?
      (d/-connection (:driver driver))))
  (-disconnect [driver]
    (assoc driver :connected? false))

  d/IExecute
  (-execute [_ sql opts]
    (d/-execute driver sql opts))

  d/IFetch
  (-fetch [_ sql opts]
    (d/-fetch driver sql opts))

  d/IPrepareStatement
  (-prepare-statement [_ sql opts]
    (d/-prepare-statement driver sql opts))

  d/ITransaction
  (-begin [_ opts]
    (d/-begin driver opts))
  (-commit [_ opts]
    (d/-commit driver opts))
  (-rollback [_ opts]
    (d/-rollback driver opts))

  component/Lifecycle
  (start [driver]
    (->> (-> (d/-connect (:driver driver) nil) (d/-begin nil))
         (assoc driver :driver)))
  (stop [driver]
    (some-> driver :driver (d/-rollback nil))
    (update driver :driver d/-disconnect)))

(defn driver
  "Return a new test driver that rolls back any changes to the
  database that happen between the component's start and stop
  life-cycle."
  [db]
  (->Driver (d/find-driver db) false))
