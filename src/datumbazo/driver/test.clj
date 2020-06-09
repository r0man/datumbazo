(ns datumbazo.driver.test
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as d]))

(defn- start-driver [{:keys [db] :as driver}]
  (let [driver (update driver :driver d/-connect db nil)
        connection (d/-connection (:driver driver) nil)]
    (.setAutoCommit connection false)
    driver))

(defn- stop-driver [{:keys [db] :as driver}]
  (.rollback (d/-connection (:driver driver) nil))
  (update driver :driver d/-disconnect db))

(defrecord Driver [db driver connected?]
  d/Connectable
  (-connect [driver db opts]
    (assoc driver :connected? true))
  (-connection [driver db]
    (when connected?
      (d/-connection (:driver driver) db)))
  (-disconnect [driver db]
    (assoc driver :connected? false))

  d/Executeable
  (-execute-all [_ db sql opts]
    (d/-execute-all driver db sql opts))
  (-execute-one [_ db sql opts]
    (d/-execute-one driver db sql opts))

  d/Preparable
  (-prepare [_ db sql opts]
    (d/-prepare driver db sql opts))

  d/Transactable
  (-transact [_ db f opts]
    (d/-transact driver db f opts))

  component/Lifecycle
  (start [driver]
    (start-driver driver))
  (stop [driver]
    (stop-driver driver)))

(defn driver
  "Return a new test driver that rolls back any changes to the
  database that happen between the component's start and stop
  life-cycle."
  [db]
  (map->Driver
   {:connected? false
    :db db
    :driver (d/driver db)}))
