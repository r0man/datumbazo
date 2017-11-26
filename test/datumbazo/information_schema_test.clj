(ns datumbazo.information-schema-test
  (:require [clojure.test :refer [deftest is]]
            [datumbazo.information-schema :as schema]
            [datumbazo.test :as t :refer [with-backends]]))

(deftest test-column
  (with-backends [db]
    (t/create-test-table db :patients)
    (is (= (schema/column db :patients.id)
           {:collation-catalog nil
            :identity-increment nil
            :identity-minimum nil
            :dtd-identifier "1"
            :identity-generation nil
            :column-default "nextval('patients_id_seq'::regclass)"
            :numeric-scale 0
            :data-type "integer"
            :identity-start nil
            :collation-schema nil
            :udt-name "int4"
            :ordinal-position 1
            :character-maximum-length nil
            :udt-catalog "datumbazo"
            :maximum-cardinality nil
            :is-self-referencing "NO"
            :scope-catalog nil
            :datetime-precision nil
            :character-octet-length nil
            :is-updatable "YES"
            :scope-schema nil
            :numeric-precision 32
            :interval-precision nil
            :character-set-catalog nil
            :table-catalog "datumbazo"
            :domain-schema nil
            :domain-catalog nil
            :udt-schema "pg_catalog"
            :character-set-name nil
            :domain-name nil
            :is-identity "NO"
            :generation-expression nil
            :table-schema "public"
            :interval-type nil
            :collation-name nil
            :character-set-schema nil
            :identity-cycle "NO"
            :identity-maximum nil
            :table-name "patients"
            :column-name "id"
            :is-generated "NEVER"
            :numeric-precision-radix 2
            :is-nullable "NO"
            :scope-name nil}))))

(deftest test-foreign-keys
  (with-backends [db]
    (t/create-test-table db :patients)
    (t/create-test-table db :physicians)
    (t/create-test-table db :appointments)
    (is (= (schema/foreign-keys db :appointments)
           [{:reference-table-name "patients",
             :constraint-name "appointments_patient-id_fkey",
             :is-deferrable "NO",
             :reference-table-catalog "datumbazo",
             :constraint-schema "public",
             :constraint-catalog "datumbazo",
             :table-catalog "datumbazo",
             :reference-table-schema "public",
             :initially-deferred "NO",
             :table-schema "public",
             :table-name "appointments",
             :constraint-type "FOREIGN KEY",
             :column-name "patient-id",
             :reference-column-name "id"}
            {:reference-table-name "physicians",
             :constraint-name "appointments_physician-id_fkey",
             :is-deferrable "NO",
             :reference-table-catalog "datumbazo",
             :constraint-schema "public",
             :constraint-catalog "datumbazo",
             :table-catalog "datumbazo",
             :reference-table-schema "public",
             :initially-deferred "NO",
             :table-schema "public",
             :table-name "appointments",
             :constraint-type "FOREIGN KEY",
             :column-name "physician-id",
             :reference-column-name "id"}]))))

(deftest test-primary-keys
  (with-backends [db]
    (t/create-test-table db :patients)
    (is (= (schema/primary-keys db :patients)
           [{:constraint-catalog "datumbazo"
             :constraint-schema "public"
             :constraint-name "patients_pkey"
             :table-catalog "datumbazo"
             :table-schema "public"
             :table-name "patients"
             :column-name "id"
             :ordinal-position 1
             :position-in-unique-constraint nil}]))))

(deftest test-unique-keys
  (with-backends [db]
    (t/create-test-table db :patients)
    (is (= (schema/unique-keys db :patients)
           [{:constraint-name "patients_name_key"
             :is-deferrable "NO"
             :constraint-schema "public"
             :constraint-catalog "datumbazo"
             :table-catalog "datumbazo"
             :initially-deferred "NO"
             :table-schema "public"
             :table-name "patients"
             :constraint-type "UNIQUE"
             :column-name "name"}]))))

(deftest test-table
  (with-backends [db]
    (t/create-test-table db :patients)
    (t/create-test-table db :physicians)
    (t/create-test-table db :appointments)
    (is (= (schema/table db :appointments)
           {:column-names ["id" "patient-id" "physician-id" "appointment-date"]
            :columns
            {"id"
             {:collation-catalog nil
              :identity-increment nil
              :identity-minimum nil
              :dtd-identifier "1"
              :identity-generation nil
              :column-default "nextval('appointments_id_seq'::regclass)"
              :numeric-scale 0
              :data-type "integer"
              :identity-start nil
              :collation-schema nil
              :udt-name "int4"
              :ordinal-position 1
              :character-maximum-length nil
              :udt-catalog "datumbazo"
              :maximum-cardinality nil
              :is-self-referencing "NO"
              :scope-catalog nil
              :datetime-precision nil
              :character-octet-length nil
              :is-updatable "YES"
              :scope-schema nil
              :numeric-precision 32
              :interval-precision nil
              :character-set-catalog nil
              :table-catalog "datumbazo"
              :domain-schema nil
              :domain-catalog nil
              :udt-schema "pg_catalog"
              :character-set-name nil
              :domain-name nil
              :is-identity "NO"
              :generation-expression nil
              :table-schema "public"
              :interval-type nil
              :collation-name nil
              :character-set-schema nil
              :identity-cycle "NO"
              :identity-maximum nil
              :table-name "appointments"
              :column-name "id"
              :is-generated "NEVER"
              :numeric-precision-radix 2
              :is-nullable "NO"
              :scope-name nil}
             "patient-id"
             {:collation-catalog nil
              :identity-increment nil
              :identity-minimum nil
              :dtd-identifier "2"
              :identity-generation nil
              :column-default nil
              :numeric-scale 0
              :data-type "integer"
              :identity-start nil
              :collation-schema nil
              :udt-name "int4"
              :ordinal-position 2
              :character-maximum-length nil
              :udt-catalog "datumbazo"
              :maximum-cardinality nil
              :is-self-referencing "NO"
              :scope-catalog nil
              :datetime-precision nil
              :character-octet-length nil
              :is-updatable "YES"
              :scope-schema nil
              :numeric-precision 32
              :interval-precision nil
              :character-set-catalog nil
              :table-catalog "datumbazo"
              :domain-schema nil
              :domain-catalog nil
              :udt-schema "pg_catalog"
              :character-set-name nil
              :domain-name nil
              :is-identity "NO"
              :generation-expression nil
              :table-schema "public"
              :interval-type nil
              :collation-name nil
              :character-set-schema nil
              :identity-cycle "NO"
              :identity-maximum nil
              :table-name "appointments"
              :column-name "patient-id"
              :is-generated "NEVER"
              :numeric-precision-radix 2
              :is-nullable "YES"
              :scope-name nil}
             "physician-id"
             {:collation-catalog nil
              :identity-increment nil
              :identity-minimum nil
              :dtd-identifier "3"
              :identity-generation nil
              :column-default nil
              :numeric-scale 0
              :data-type "integer"
              :identity-start nil
              :collation-schema nil
              :udt-name "int4"
              :ordinal-position 3
              :character-maximum-length nil
              :udt-catalog "datumbazo"
              :maximum-cardinality nil
              :is-self-referencing "NO"
              :scope-catalog nil
              :datetime-precision nil
              :character-octet-length nil
              :is-updatable "YES"
              :scope-schema nil
              :numeric-precision 32
              :interval-precision nil
              :character-set-catalog nil
              :table-catalog "datumbazo"
              :domain-schema nil
              :domain-catalog nil
              :udt-schema "pg_catalog"
              :character-set-name nil
              :domain-name nil
              :is-identity "NO"
              :generation-expression nil
              :table-schema "public"
              :interval-type nil
              :collation-name nil
              :character-set-schema nil
              :identity-cycle "NO"
              :identity-maximum nil
              :table-name "appointments"
              :column-name "physician-id"
              :is-generated "NEVER"
              :numeric-precision-radix 2
              :is-nullable "YES"
              :scope-name nil}
             "appointment-date"
             {:collation-catalog nil
              :identity-increment nil
              :identity-minimum nil
              :dtd-identifier "4"
              :identity-generation nil
              :column-default nil
              :numeric-scale nil
              :data-type "timestamp with time zone"
              :identity-start nil
              :collation-schema nil
              :udt-name "timestamptz"
              :ordinal-position 4
              :character-maximum-length nil
              :udt-catalog "datumbazo"
              :maximum-cardinality nil
              :is-self-referencing "NO"
              :scope-catalog nil
              :datetime-precision 6
              :character-octet-length nil
              :is-updatable "YES"
              :scope-schema nil
              :numeric-precision nil
              :interval-precision nil
              :character-set-catalog nil
              :table-catalog "datumbazo"
              :domain-schema nil
              :domain-catalog nil
              :udt-schema "pg_catalog"
              :character-set-name nil
              :domain-name nil
              :is-identity "NO"
              :generation-expression nil
              :table-schema "public"
              :interval-type nil
              :collation-name nil
              :character-set-schema nil
              :identity-cycle "NO"
              :identity-maximum nil
              :table-name "appointments"
              :column-name "appointment-date"
              :is-generated "NEVER"
              :numeric-precision-radix nil
              :is-nullable "YES"
              :scope-name nil}}
            :foreign-keys
            {"patient-id"
             {:reference-table-name "patients"
              :constraint-name "appointments_patient-id_fkey"
              :is-deferrable "NO"
              :reference-table-catalog "datumbazo"
              :constraint-schema "public"
              :constraint-catalog "datumbazo"
              :table-catalog "datumbazo"
              :reference-table-schema "public"
              :initially-deferred "NO"
              :table-schema "public"
              :table-name "appointments"
              :constraint-type "FOREIGN KEY"
              :column-name "patient-id"
              :reference-column-name "id"}
             "physician-id"
             {:reference-table-name "physicians"
              :constraint-name "appointments_physician-id_fkey"
              :is-deferrable "NO"
              :reference-table-catalog "datumbazo"
              :constraint-schema "public"
              :constraint-catalog "datumbazo"
              :table-catalog "datumbazo"
              :reference-table-schema "public"
              :initially-deferred "NO"
              :table-schema "public"
              :table-name "appointments"
              :constraint-type "FOREIGN KEY"
              :column-name "physician-id"
              :reference-column-name "id"}}
            :op :table
            :primary-keys {}
            :table-name "appointments"
            :table-schema "public"
            :unique-keys {}}))))
