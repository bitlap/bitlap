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
package org.bitlap.server.http

import org.bitlap.server.http.model.*

import sttp.tapir.{ ValidationResult, Validator }

/** Validating forms and data passed from the frontend.
 */
trait FormValidator {

  lazy val LogoutValidator: Validator[UserLogoutInput] =
    Validator.custom[UserLogoutInput](in =>
      ValidationResult.validWhen(
        in.username.trim.nonEmpty && in.username.toLongOption.isEmpty
      )
    )

  lazy val LoginValidator: Validator[UserLoginInput] =
    Validator.custom[UserLoginInput](in =>
      ValidationResult.validWhen(
        in.username.trim.nonEmpty && in.username.toLongOption.isEmpty
      )
    )

  lazy val NameValidator: Validator[String] =
    Validator.custom[String](s =>
      ValidationResult.validWhen(
        s.trim.nonEmpty && s.toLongOption.isEmpty
      )
    )

}
