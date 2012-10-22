# Datumbazo

Clojure Database Kung-Foo.

## Installation

Via Clojars: http://clojars.org/datumbazo

## Examples

### Select statement

Select an expression from the database.

    (sql (select 1))
    ;=> ["SELECT 1"]

Select all rows from the continents table.

    (sql (-> (select *)
             (from :continents)))
    ;=> ["SELECT * FROM continents"]

Select only the id and name columns from the rows in the continents table.

    (sql (-> (select :id :name)
             (from :continents)))
    ;=> ["SELECT id, name FROM continents"]

## License

Copyright (C) 2011 Roman Scherer

Distributed under the Eclipse Public License, the same as Clojure.
