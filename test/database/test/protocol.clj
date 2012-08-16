(ns database.test.protocol
  (:require [inflections.core :refer [dasherize underscore]])
  (:use clojure.test
        database.connection
        database.protocol
        database.schema))

(deftest test-as-keyword
  (are [obj expected]
    (is (= expected (as-keyword obj)))
    nil nil
    :weather-models :weather-models
    "weather-models" :weather-models
    "weather_models" :weather_models)
  (with-naming-strategy {:keyword dasherize}
    (are [obj expected]
      (is (= expected (as-keyword obj)))
      nil nil
      :weather-models :weather-models
      "weather-models" :weather-models
      "weather_models" :weather-models)))

(deftest test-as-identifier
  (are [obj expected]
    (is (= expected (as-identifier obj)))
    nil nil
    :weather-models "weather-models"
    "weather-models" "weather-models"
    "weather_models" "weather_models")
  (with-quoted-identifiers \"
    (are [obj expected]
      (is (= expected (as-identifier obj)))
      nil nil
      :weather-models "\"weather-models\""
      "weather-models" "weather-models"
      "weather_models" "weather_models"))
  (with-naming-strategy {:entity underscore}
    (are [obj expected]
      (is (= expected (as-identifier obj)))
      nil nil
      :weather-models "weather_models"
      "weather-models" "weather-models"
      "weather_models" "weather_models")))
