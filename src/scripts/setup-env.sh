#!/bin/bash
#
# Copyright 2010 Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

VERSION="@VERSION@"

if java -version 2>&1 |grep "1\.5"; then
  echo "Java must be at least 1.6"
  exit 1
fi

if [ "x$DB_USERNAME" = "x" ]; then
  echo "Please set DB_USERNAME and/or DB_PASSWORD."
  exit 1
fi

if gizzmo --help > /dev/null; then
  gizzmo="gizzmo -H localhost -P 7920"
else
  echo "Make sure you have gizzmo available on your path."
  echo "Find it here: http://github.com/twitter/gizzmo"
  exit 1
fi

MYSQL_COMMAND=$(if [ "x$DB_PASSWORD" = "x" ]; then
  echo "mysql -u$DB_USERNAME"
else
  echo "mysql -u$DB_USERNAME -p$DB_PASSWORD"
fi)

function exec_sql {
  echo $1 | $MYSQL_COMMAND
}

echo "Killing any running flockdb..."
curl http://localhost:9990/shutdown >/dev/null 2>/dev/null
sleep 3

echo "Launching flockdb..."
exec_sql "DROP DATABASE IF EXISTS flockdb_development"
exec_sql "CREATE DATABASE IF NOT EXISTS flockdb_development"

JAVA_OPTS="-Xms256m -Xmx256m -XX:NewSize=64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -server"
java $JAVA_OPTS -jar ./dist/flockdb/flockdb-${VERSION}.jar config/development.scala &
sleep 10

echo "Creating shards..."
i=1
while [ $i -le 15 ]; do
  /bin/echo -n "$i "
  exec_sql "DROP TABLE IF EXISTS edges_development.forward_${i}_0000_edges"
  exec_sql "DROP TABLE IF EXISTS edges_development.forward_${i}_0000_metadata"
  exec_sql "DROP TABLE IF EXISTS edges_development.backward_${i}_0000_edges"
  exec_sql "DROP TABLE IF EXISTS edges_development.backward_${i}_0000_metadata"
  forward_shard=$($gizzmo create -s "INT UNSIGNED" -d "INT UNSIGNED" "com.twitter.flockdb.SqlShard" "localhost/forward_${i}_0000")
  backward_shard=$($gizzmo create -s "INT UNSIGNED" -d "INT UNSIGNED" "com.twitter.flockdb.SqlShard" "localhost/backward_${i}_0000")
  $gizzmo addforwarding -- $i 0 $forward_shard
  $gizzmo addforwarding -- -$i 0 $backward_shard
  i=$((i + 1))
done
echo
$gizzmo -f reload
echo "Done."
