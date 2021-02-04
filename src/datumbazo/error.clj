(ns datumbazo.error
  (:require [clojure.spec.alpha :as s])
  (:import java.sql.SQLException))

(s/def ::code int?)
(s/def ::message string?)
(s/def ::sql vector?)
(s/def ::state string?)
(s/def ::type qualified-keyword?)

(s/def :datumbazo/error
  (s/keys :req [::code ::message ::state ::type]
          :req-un [::type]
          :opt [::sql]))

(defmulti message
  (fn [exception] (class exception)))

(defmethod message SQLException [^SQLException exception]
  (ex-message exception))

(defmulti data
  (fn [exception] (class exception)))

(defmethod data SQLException [^SQLException exception]
  {:datumbazo.error/cause (.getCause exception)
   :datumbazo.error/code (.getErrorCode exception)
   :datumbazo.error/message (ex-message exception)
   :datumbazo.error/state (.getSQLState exception)
   :datumbazo.error/type :datumbazo.error/sql})

(defn error
  "Return a general exception info."
  [^SQLException exception]
  (ex-info (message exception)
           (merge (data exception)
                  {:type :datumbazo/error})
           exception))

(defn sql-error
  "Return a SQL exception info."
  [^SQLException exception sql]
  (ex-info (message exception)
           (merge (data exception)
                  {:datumbazo.error/sql sql
                   :type :datumbazo/error})
           exception))
