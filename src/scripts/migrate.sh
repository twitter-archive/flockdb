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

function find_java() {
  if [ ! -z "$JAVA_HOME" ]; then
    return
  fi
  for dir in /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/default; do
    if [ -x $dir/bin/java ]; then
      JAVA_HOME=$dir
      break
    fi
  done
}

find_java

JAVA_OPTS="-Xms256m -Xmx1024m -XX:NewSize=128m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -server"
DIST_HOME="$(dirname $0)/.."

$JAVA_HOME/bin/java $JAVA_OPTS -Dstage=migrate -jar $DIST_HOME/flockdb-$VERSION.jar migrate "$@"
