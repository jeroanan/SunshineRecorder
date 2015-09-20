#!/bin/bash

scalac -deprecation -feature -classpath class/*.jar -d bin src/Sqlite/*.scala src/*.scala src/sunshine/*.scala
