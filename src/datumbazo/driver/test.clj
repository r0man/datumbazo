(ns datumbazo.driver.test
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :refer :all]))

(defrecord Driver [driver connected?])

(defn driver
  "Return a new test driver that rolls back any changes to the
  database that happen between the component's start and stop
  life-cycle."
  [db]
  (->Driver (find-driver db) (atom false)))

(extend-protocol IConnection
  Driver
  (-connect [driver opts]
    (when-not @(:connected? driver)
      (reset! (:connected? driver) true))
    driver)
  (-connection [driver]
    (when @(:connected? driver)
      (-connection (:driver driver))))
  (-disconnect [driver]
    (when @(:connected? driver)
      (reset! (:connected? driver) false))
    driver))

(extend-protocol IExecute
  Driver
  (-execute [driver sql opts]
    (-execute (:driver driver) sql opts)))

(extend-protocol IFetch
  Driver
  (-fetch [driver sql opts]
    (-fetch (:driver driver) sql opts)))

(extend-protocol IPrepareStatement
  Driver
  (-prepare-statement [driver sql opts]
    (-prepare-statement (:driver driver) sql opts)))

(extend-protocol ITransaction
  Driver
  (-begin [driver opts]
    (-begin (:driver driver) opts))
  (-commit [driver opts]
    (-commit (:driver driver) opts))
  (-rollback [driver opts]
    (-rollback (:driver driver) opts)))

(extend-protocol component/Lifecycle
  Driver
  (start [driver]
    (->> (-> (-connect (:driver driver) nil)
             (-begin nil)
             (-rollback nil))
         (assoc driver :driver )))
  (stop [driver]
    (some-> driver :driver -connection .rollback)
    (assoc driver :driver (-disconnect (:driver driver)))))
