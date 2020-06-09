(ns datumbazo.table-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.table :as table]
            [datumbazo.test :refer :all]
            [datumbazo.util :as util]
            [sqlingvo.core :as sql])
  (:import datumbazo.continents.Continent))

(def continents-table
  (util/table-by-class Continent))

(deftest test-define-truncate
  (is (= (#'table/define-truncate continents-table)
         '(clojure.core/defn truncate!
            "Truncate the continents table."
            [db & [opts]]
            @(sqlingvo.core/truncate db [:continents]
               (sqlingvo.core/cascade (:cascade opts)))))))
