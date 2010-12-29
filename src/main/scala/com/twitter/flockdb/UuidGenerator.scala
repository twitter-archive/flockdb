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

import java.util.Random

trait UuidGenerator extends (Long => Long) {
  def apply(updatedAt: Long): Long
  def unapply(uuid: Long): Option[Long]
}

object OrderedUuidGenerator extends UuidGenerator {
  private val randomGenerator = new Random
  // 64 bits - 20 leaves 44 bits of milliseconds, or over 500 years.
  private val unusedBits = 20
  private val randomMask = (1 << unusedBits) - 1

  def apply(updatedAt: Long) = {
    (updatedAt << unusedBits) | (randomGenerator.nextInt() & randomMask)
  }

  def unapply(uuid: Long) = {
    Some(uuid >> unusedBits)
  }
}

object IdentityUuidGenerator extends UuidGenerator {
  def apply(updatedAt: Long) = updatedAt

  def unapply(uuid: Long) = Some(uuid)
}
