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

abstract sealed class Direction(val id: Int) {
  val opposite: Direction
}

object Direction {
  def apply(id: Int) = id match {
    case Forward.id => Forward
    case Backward.id => Backward
  }

  def apply(isForward: Boolean) = if (isForward) Forward else Backward

  case object Forward extends Direction(0) {
    val opposite = Direction.Backward
  }

  case object Backward extends Direction(1) {
    val opposite = Direction.Forward
  }

  val All = List(Forward, Backward)
}
