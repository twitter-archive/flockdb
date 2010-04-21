#!/bin/sh

${DB_USERNAME?"Please set DB_USERNAME"}
${DB_PASSWORD?"Please set DB_PASSWORD"}

echo "Killing any running flockdb..."
curl http://localhost:9990/shutdown >/dev/null 2>/dev/null
sleep 3

echo "Launching flock..."
ant -q launch
sleep 5

echo "Creating shards..."
echo "CREATE DATABASE IF NOT EXISTS flockdb_development" | mysql -u$DB_USERNAME -p$DB_PASSWORD
i=1
while [ $i -lt 15 ]; do
  echo "DROP TABLE IF EXISTS edges_development.forward_${i}_edges" | mysql -u$DB_USERNAME -p$DB_PASSWORD
  echo "DROP TABLE IF EXISTS edges_development.forward_${i}_metadata" | mysql -u$DB_USERNAME -p$DB_PASSWORD
  echo "DROP TABLE IF EXISTS edges_development.backward_${i}_edges" | mysql -u$DB_USERNAME -p$DB_PASSWORD
  echo "DROP TABLE IF EXISTS edges_development.backward_${i}_metadata" | mysql -u$DB_USERNAME -p$DB_PASSWORD
  i=$((i + 1))
done

./src/scripts/flocker.rb -D setup-dev
