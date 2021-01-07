# Presto [![Build Status](https://travis-ci.org/prestodb/presto.svg?branch=master)](https://travis-ci.org/prestodb/presto)

Presto is a distributed SQL query engine for big data.

See the [User Manual](https://prestodb.github.io/docs/current/) for deployment instructions and end user documentation.

## Requirements

* Mac OS X or Linux
* Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported.
* Maven 3.3.9+ (for building)
* Python 2.4+ (for running with the launcher script)

## Building Presto

Presto is a standard Maven project. Simply run the following command from the project root directory:

    ./mvnw clean install

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    ./mvnw clean install -DskipTests

# Parsing queries to get table names

This was created to get metadata from tableau queries. It's an opinionated piece of code, not meant to work out of the box. But it will work with modifications. The primary aim of how to parse a presto query to get table names should be fulfilled.

There are 3 files inside presto-verifier directory:
 - Parse.java :: Reads from a csv, takes the query, gets the table names and writes a csv back.
 - RunParser.java :: Creates a sqlparser and gets table names from the query.
 - QueryVisitor.java :: Extends ASTVisitor and overrides some functions. Gets the table names and returns a set of table names.
