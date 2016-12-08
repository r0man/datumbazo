(ns datumbazo.types
  (:require [clojure.spec :as s]
            [clojure.spec.gen :as gen]))

;; Numeric types

(s/def ::smallint
  (s/int-in -32768 32767))

(s/def ::integer
  (s/int-in -2147483648 2147483647))

(s/def ::bigint
  (s/int-in -9223372036854775808 9223372036854775807))

(s/def ::decimal double?)

(s/def ::numeric double?)

(s/def ::real double?)

(s/def ::double-precision double?)

(s/def ::smallserial
  (s/int-in 1 32767))

(s/def ::serial
  (s/int-in 1 2147483647))

(s/def ::bigserial
  (s/int-in 1 9223372036854775807))

;; Character Types

(s/def ::character-varying string?)

(s/def ::character string?)

(s/def ::citext string?)

(s/def ::text string?)

(s/def ::varchar string?)

(s/def ::timestamp inst?)

;; TODO:

(s/def ::geometry nil?)
