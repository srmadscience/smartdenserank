# README #

This README explains how to use this demo

## What is this repository for? ##

* The repo contains the code required to run a dense rank measurng system where the number of contestants is large.
* Version 1.0

## How do I get set up? ##

* Create a new VoltDB database
* Compile the classes in serverSrc and put them in a JAR file called smartdenserank.jar next to the top level directory
* Create the schema :

```
cd smartdenserank/ddl
sqlcmd < ddl.sql
```

Run the 'Demo' class in clientSrc. It assumes VoltDB is on 127.0.0.1
