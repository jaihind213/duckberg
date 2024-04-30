#!/bin/bash

echo "need java17"
sleep 4
for i in {1..16}; do
	java -Xms9g -Xmx9g --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -cp target/duckberg-1.0-SNAPSHOT-jar-with-dependencies.jar org.duckberg.QuerySparkIceBerg query_$i  
done
