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
package org.bitlap.network

import com.typesafe.scalalogging.LazyLogging

import io.grpc.*

type Identity[T] = T

private[bitlap] final case class ServerAddress(ip: String, port: Int)

lazy val errorApplyFunc: Throwable => StatusException = (ex: Throwable) => new StatusException(Status.fromThrowable(ex))

extension [R <: AutoCloseable](r: R) def use[T](func: R => T): T = scala.util.Using.resource(r)(func)

end extension
