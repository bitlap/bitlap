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
package org.bitlap.server.http.model

import java.nio.charset.StandardCharsets
import java.util.Base64

inline val DefaultPassword = ""

final case class UserLoginInput(username: String, password: Option[String])
final case class UserLogoutInput(username: String)

final case class AccountInfo(
  username: String,
  avatar: String,
  email: Option[String] = None,
  nickName: Option[String] = None)

object AccountInfo:

  def createCookieValue(username: String, password: String): String = {
    val base64 = Base64.getEncoder.encode(s"Bearer $username:$password".getBytes(StandardCharsets.UTF_8))
    new String(base64)
  }

  val root: AccountInfo = AccountInfo(
    "root",
    "https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png"
  )

final case class UserLogout()
