/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.flockdb
package unit

import com.twitter.util.Time
import org.specs.mock.{ClassMocker, JMocker}
import shards.{ReadWriteShardAdapter, Shard}
import thrift.Edge


object ReadWriteShardAdapterSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  var shard1: Shard = null
  var shard2: Shard = null
  var shard3: Shard = null

  "ReadWriteShardAdapter" in {
  }
}
