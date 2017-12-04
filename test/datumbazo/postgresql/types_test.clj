(ns datumbazo.postgresql.types-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as prop]
            [datumbazo.postgresql.types :as t]))

(deftest test-geometry
  (is (s/exercise ::t/geometry)))

(deftest test-geography
  (is (s/exercise ::t/geography)))

(deftest test-smallint-minimum
  (prop/for-all [x (s/gen ::t/smallint)] (>= x -32768)))

(defspec test-smallint-maximum
  (prop/for-all [x (s/gen ::t/smallint)] (<= x 32767)))

(defspec test-integer-minimum
  (prop/for-all [x (s/gen ::t/integer)] (>= x -2147483648)))

(defspec test-integer-maximum
  (prop/for-all [x (s/gen ::t/integer)] (<= x 2147483647)))

(defspec test-bigint-minimum
  (prop/for-all [x (s/gen ::t/bigint)] (>= x -9223372036854775808)))

(defspec test-bigint-maximum
  (prop/for-all [x (s/gen ::t/bigint)] (<= x 9223372036854775807)))

(defspec test-decimal
  (prop/for-all [x (s/gen ::t/decimal)] (double? x)))

(defspec test-numeric
  (prop/for-all [x (s/gen ::t/numeric)] (double? x)))

(defspec test-real
  (prop/for-all [x (s/gen ::t/real)] (double? x)))

(defspec test-double-precision
  (prop/for-all [x (s/gen ::t/double-precision)] (double? x)))

(defspec test-smallserial-minimum
  (prop/for-all [x (s/gen ::t/smallserial)] (>= x 1)))

(defspec test-smallserial-maximum
  (prop/for-all [x (s/gen ::t/smallserial)] (<= x 32767)))

(defspec test-serial-minimum
  (prop/for-all [x (s/gen ::t/serial)] (>= x 1)))

(defspec test-serial-maximum
  (prop/for-all [x (s/gen ::t/serial)] (<= x 2147483647)))

(defspec test-bigserial-minimum
  (prop/for-all [x (s/gen ::t/bigserial)] (>= x 1)))

(defspec test-bigserial-maximum
  (prop/for-all [x (s/gen ::t/bigserial)] (<= x 9223372036854775807)))

(defspec test-character-varying
  (prop/for-all [x (s/gen ::t/character-varying)] (string? x)))

(defspec test-character
  (prop/for-all [x (s/gen ::t/character)] (string? x)))

(defspec test-text
  (prop/for-all [x (s/gen ::t/text)] (string? x)))

(defspec test-varchar
  (prop/for-all [x (s/gen ::t/varchar)] (string? x)))

(defspec test-timestamp
  (prop/for-all [x (s/gen ::t/timestamp)] (inst? x)))
