/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.server.http.routes

import scala.collection.mutable.ListBuffer

import sttp.tapir.{ AnyEndpoint, Endpoint }
import sttp.tapir.ztapir.*
import zio.ZIO

trait Route(name: String) {

  protected lazy val API: Endpoint[Unit, Unit, Unit, Unit, Any]                      = endpoint.in("api" / name)
  protected lazy val endpoints: ListBuffer[(AnyEndpoint, ZServerEndpoint[Any, Any])] = ListBuffer.empty

  protected def get[A, I, E, O, R, RR](
    point: Endpoint[Unit, Unit, Unit, Unit, Any] => Endpoint[A, I, E, O, R]
  )(
    logic: I => ZIO[RR, E, O]
  )(using aIsUnit: A =:= Unit
  ): ZServerEndpoint[RR, R] = {
    val ed     = point(API.get)
    val zLogic = ed.zServerLogic[RR](logic)(using aIsUnit)
    endpoints += ed -> zLogic.asInstanceOf[ZServerEndpoint[Any, Any]]
    zLogic
  }

  protected def post[A, I, E, O, R, RR](
    point: Endpoint[Unit, Unit, Unit, Unit, Any] => Endpoint[A, I, E, O, R]
  )(
    logic: I => ZIO[RR, E, O]
  )(using aIsUnit: A =:= Unit
  ): ZServerEndpoint[RR, R] = {
    val ed     = point(API.post)
    val zLogic = ed.zServerLogic[RR](logic)(using aIsUnit)
    endpoints += ed -> zLogic.asInstanceOf[ZServerEndpoint[Any, Any]]
    zLogic
  }

  def getEndpoints: ListBuffer[(AnyEndpoint, ZServerEndpoint[Any, Any])] = endpoints
}
