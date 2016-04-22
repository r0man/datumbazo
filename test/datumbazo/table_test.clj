(ns datumbazo.table-test
  (:require [clojure.test :refer :all]
            [datumbazo.table :refer :all]
            [datumbazo.test :refer :all]
            [datumbazo.util :as util]
            [sqlingvo.core :as sql])
  (:import datumbazo.continents.Continent))

(def continents-table
  (util/table-by-class Continent))

(deftest test-define-truncate
  (is (= (#'datumbazo.table/define-truncate continents-table)
         '(clojure.core/defn truncate!
            "Truncate the continents table."
            [db & [opts]]
            @(sqlingvo.core/truncate db [:continents]
               (sqlingvo.core/cascade (:cascade opts)))))))
