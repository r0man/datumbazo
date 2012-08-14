(ns database.test.protocol
  (:use clojure.test
        database.connection
        database.protocol
        database.schema))

(deftest test-as-identifier
  (are [obj expected]
    (is (= expected (as-identifier obj)))
    :weather-models "weather-models"
    "weather-models" "weather-models")
  (with-quoted-identifiers \"
    (are [obj expected]
      (is (= expected (as-identifier obj)))
      :weather-models "\"weather-models\""
      "weather-models" "weather-models")))
