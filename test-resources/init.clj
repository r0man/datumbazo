(def database-environments
  {:development
   {:database
    {:classname "org.postgresql.Driver"
     :subprotocol "postgresql"
     :subname "//localhost/database_development"
     :user "database"
     :password "database"}}
   :test
   {:database
    {:classname "org.postgresql.Driver"
     :subprotocol "postgresql"
     :subname "//localhost/database_test"
     :user "database"
     :password "database"}}})
