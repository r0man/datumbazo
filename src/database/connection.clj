(ns database.connection
  (:use korma.db))

(defn connection
  "Returns a connection from the current database spec."
  [] (if-let [connection (get-connection @_default)]
       connection (throw (Exception. "Can't find current database connection."))))

(defn connection-spec
  "Returns the current connection spec."
  [] (if-let [spec @_default]
       spec (throw (Exception. "Can't find current database connection spec."))))

(defn naming-strategy
  "Returns naming strategy of the current connection spec."
  [] (merge {:keys identity :fields identity}
            (:naming (:options (connection-spec)))))
