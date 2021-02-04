(ns datumbazo.db.postgresql.error
  (:require [clojure.string :as str]
            [datumbazo.error :as error])
  (:import [org.postgresql.util PSQLException PSQLState]))

(defn- state-keyword
  "Return the keyword for the PostgreSQL `state`."
  [^PSQLState state]
  (keyword "datumbazo.postgresql.error"
           (str/replace (str/lower-case (.name state)) "_" "-")))

(def ^:private state->keyword
  "Convert a PostgreSQL state string to an error keyword."
  (zipmap (map #(.getState %) (PSQLState/values))
          (map state-keyword (PSQLState/values))))

(defmethod error/data PSQLException [^PSQLException exception]
  {:datumbazo.error/code (.getErrorCode exception)
   :datumbazo.error/message (error/message exception)
   :datumbazo.error/state (.getSQLState exception)
   :datumbazo.error/type (state->keyword (.getSQLState exception))})

(defmethod error/message PSQLException [^PSQLException exception]
  (if-let [server-error (.getServerErrorMessage exception)]
    (str (str/capitalize (.getMessage server-error)) ".")
    (.getMessage exception)))
