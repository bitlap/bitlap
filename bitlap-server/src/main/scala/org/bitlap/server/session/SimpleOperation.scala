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
package org.bitlap.server.session

import java.sql.*

import scala.collection.mutable.ListBuffer

import org.bitlap.common.BitlapLogging
import org.bitlap.common.exception._
import org.bitlap.core.*
import org.bitlap.core.sql.QueryExecution
import org.bitlap.network.enumeration.*
import org.bitlap.network.models.*
import org.bitlap.network.serde.BitlapSerde
import org.bitlap.server.config.BitlapConfigWrapper

import zio.*

/** Bitlap operation implementation on a single machine
 */
final class SimpleOperation(
  parentSession: Session,
  opType: OperationType,
  hasResultSet: Boolean = false
)(using globalConfig: BitlapConfigWrapper)
    extends Operation(parentSession, opType, hasResultSet, globalConfig)
    with BitlapLogging {

  override def run(): Task[Unit] = {
    for {
      _ <- ZIO.attemptBlocking(super.setState(OperationState.RunningState))
      _ <- parentSession.currentSchemaRef.getAndUpdate { schema =>
        try {
          val execution = new QueryExecution(statement, schema.get()).execute()
          execution match
            case DefaultQueryResult(data, currentSchema) =>
              schema.set(currentSchema)
              cache.put(opHandle, data.mapTo)
            case _ =>
          super.setState(OperationState.FinishedState)
        } catch {
          case e: Throwable =>
            super.setState(OperationState.ErrorState)
            logger.error("Simple operation run failed", e)
        }
        schema
      }
    } yield ()
  }

}
