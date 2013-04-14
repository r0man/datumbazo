(ns datumbazo.test
  (:require [datumbazo.connection :refer [connection-spec connection-pool]]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest]]
            [environ.core :refer [env]]))

(def $db (env :test-db))

;; (def $db
;;   (-> (env :test-db)
;;       (connection-spec)
;;       (connection-pool)
;;       :spec))

(defmacro database-test
  "Define a database test."
  [name & body]
  `(deftest ~name
     (jdbc/db-transaction
      [~'$db ~$db]
      (jdbc/db-set-rollback-only! ~'$db)
      ~@body)))
