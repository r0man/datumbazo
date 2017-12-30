(ns datumbazo.driver.test
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as d]))

(defrecord Driver [db driver connected?]
  d/IConnection
  (-connect [this db opts]
    (-> (assoc this :connected? true)
        (update :driver d/-connect db opts)))
  (-connection [this db]
    (when connected?
      (d/-connection driver db)))
  (-disconnect [this db]
    (assoc this :connected? false))

  d/IExecute
  (-execute [_ db sql opts]
    (d/-execute driver db sql opts))

  d/IFetch
  (-fetch [_ db sql opts]
    (d/-fetch driver db sql opts))

  d/IPrepareStatement
  (-prepare-statement [_ db sql opts]
    (d/-prepare-statement driver db sql opts))

  d/ITransaction
  (-begin [_ db opts]
    (d/-begin driver db opts))
  (-commit [_ db opts]
    (d/-commit driver db opts))
  (-rollback [_ db opts]
    (d/-rollback driver db opts))

  component/Lifecycle
  (start [this]
    (->> (-> (d/-connect driver db nil)
             (d/-begin db nil))
         (assoc this :driver)))
  (stop [this]
    (-> (update this :driver d/-rollback db nil)
        (update :driver d/-disconnect db))))

(defn driver
  "Return a new test driver that rolls back any changes to the
  database that happen between the component's start and stop
  life-cycle."
  [db]
  (->Driver db (d/find-driver db) false))
