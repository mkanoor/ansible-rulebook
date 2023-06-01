#!/bin/bash

f=$(pip show drools_jpy | grep Location: | cut -d " " -f 2)/drools/jars/drools-ansible-rulebook-integration-runtime-1.0.2-SNAPSHOT.jar
curl -L -o $f https://github.com/kiegroup/drools-ansible-rulebook-integration/releases/download/latest/drools-ansible-rulebook-integration-runtime-1.0.2-SNAPSHOT.jar
