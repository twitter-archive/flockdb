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
package conversions

import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import com.twitter.flockdb


object Metadata {
  class RichFlockMetadata(metadata: flockdb.Metadata) {
    def toThrift = new thrift.Metadata(metadata.sourceId, metadata.state.id, metadata.count,
                                   metadata.updatedAt.inSeconds)
  }
  implicit def richFlockMetadata(metadata: flockdb.Metadata) = new RichFlockMetadata(metadata)

  class RichThriftMetadata(metadata: thrift.Metadata) {
    def fromThrift = new flockdb.Metadata(metadata.source_id, State(metadata.state_id), metadata.count, Time(metadata.updated_at.seconds))
  }
  implicit def richThriftMetadata(metadata: thrift.Metadata) = new RichThriftMetadata(metadata)
}