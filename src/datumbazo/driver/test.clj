(ns datumbazo.driver.test
  (:require [com.stuartsierra.component :as component]
            [datumbazo.driver.core :as d]))

;; (defrecord Driver [driver connected?]
;;   d/IConnection
;;   (-connect [driver opts]
;;     (assoc driver :connected? true))
;;   (-connection [driver]
;;     (when connected?
;;       (d/-connection (:driver driver))))
;;   (-disconnect [driver]
;;     (assoc driver :connected? false))

;;   d/IExecute
;;   (-execute [_ sql opts]
;;     (d/-execute driver sql opts))

;;   d/IFetch
;;   (-fetch [_ sql opts]
;;     (d/-fetch driver sql opts))

;;   d/IPrepareStatement
;;   (-prepare-statement [_ sql opts]
;;     (d/-prepare-statement driver sql opts))

;;   d/ITransaction
;;   (-begin [_ opts]
;;     (d/-begin driver opts))
;;   (-commit [_ opts]
;;     (d/-commit driver opts))
;;   (-rollback [_ opts]
;;     (d/-rollback driver opts))

;;   component/Lifecycle
;;   (start [driver]
;;     (->> (-> (d/-connect (:driver driver) nil) (d/-begin nil))
;;          (assoc driver :driver)))
;;   (stop [driver]
;;     (some-> driver :driver (d/-rollback nil))
;;     (update driver :driver d/-disconnect)))


(defrecord Driver [driver connected?]
  d/IConnection
  (-connect [this opts]
    (assoc this :connected? true))
  (-connection [this]
    (when (:connected? this)
      (d/connection driver)))
  (-disconnect [this]
    (assoc this :connected? false))

  d/IExecute
  (-execute [this sql opts]
    (d/-execute driver sql opts))

  d/IFetch
  (-fetch [this sql opts]
    (d/-fetch driver sql opts))

  d/IPrepareStatement
  (-prepare-statement [this sql opts]
    (d/-prepare-statement driver sql opts))

  d/ITransaction
  (-begin [this opts]
    (update this :driver d/-begin opts))
  (-commit [this opts]
    (update this :driver d/-commit opts))
  (-rollback [this opts]
    (update this :driver d/-rollback opts))

  component/Lifecycle
  (start [this]
    (-> (update this :driver d/connect nil)
        (update :driver d/begin)))
  (stop [this]
    (-> (update this :driver d/rollback)
        (update :driver d/disconnect))))

(defn driver
  "Return a new test driver that rolls back any changes to the
  database that happen between the component's start and stop
  life-cycle."
  [db]
  (->Driver (d/find-driver db) false))
