#!/bin/bash

mvn clean test -Dmaven.test.failure.ignore=true -q

mvn sonar:sonar \
  -Dsonar.projectKey="dt-insight-engine/flinkStreamSQL"  \
  -Dsonar.login=11974c5e9a29625efa09fdc3c3fdc031efb1aab1 \
  -Dsonar.host.url=http://172.16.100.198:9000 \
  -Dsonar.jdbc.url=jdbc:postgresql://172.16.100.198:5432/sonar \
  -Dsonar.java.binaries=target/classes \
  -Dsonar.inclusions="src/main/java/com/dtstack/flink/**/*" \
  -Dsonar.exclusions="src/main/java/org/**/*"
