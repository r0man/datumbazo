(ns datumbazo.driver.test
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as d]))

(defrecord Driver [driver connected?])

(defn driver
  "Return a new test driver that rolls back any changes to the
  database that happen between the component's start and stop
  life-cycle."
  [db]
  (->Driver (d/find-driver db) false))

(defn- connected?
  "Returns true if `driver` is connected, otherwise false."
  [driver]
  (:connected? driver))

(extend-protocol d/IConnection
  Driver
  (-connect [driver opts]
    {:pre [(not (connected? driver))]}
    (assoc driver :connected? true))
  (-connection [driver]
    (when (connected? driver)
      (d/-connection (:driver driver))))
  (-disconnect [driver]
    {:pre [(connected? driver)]}
    (assoc driver :connected? false)))

(extend-protocol d/IExecute
  Driver
  (-execute [driver sql opts]
    {:pre [(connected? driver)]}
    (d/-execute (:driver driver) sql opts)))

(extend-protocol d/IFetch
  Driver
  (-fetch [driver sql opts]
    {:pre [(connected? driver)]}
    (d/-fetch (:driver driver) sql opts)))

(extend-protocol d/IPrepareStatement
  Driver
  (-prepare-statement [driver sql opts]
    {:pre [(connected? driver)]}
    (d/-prepare-statement (:driver driver) sql opts)))

(extend-protocol d/ITransaction
  Driver
  (-begin [driver opts]
    {:pre [(connected? driver)]}
    (d/-begin (:driver driver) opts))
  (-commit [driver opts]
    {:pre [(connected? driver)]}
    (d/-commit (:driver driver) opts))
  (-rollback [driver opts]
    {:pre [(connected? driver)]}
    (d/-rollback (:driver driver) opts)))

(extend-protocol component/Lifecycle
  Driver
  (start [driver]
    (->> (-> (d/-connect (:driver driver) nil)
             (d/-begin nil)
             (d/-rollback nil))
         (assoc driver :driver )))
  (stop [driver]
    (some-> driver :driver d/-connection .rollback)
    (assoc driver :driver (d/-disconnect (:driver driver)))))
