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
package org.bitlap.common.conf

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object Validators {

    val NOT_BLANK = Validator<String> { t -> !t.isNullOrBlank() }
    fun <T : Comparable<T>> eq(v: T) = Validator<T> { t -> t != null && t.compareTo(v) == 0 }
    fun <T : Comparable<T>> neq(v: T) = Validator<T> { t -> t != null && t.compareTo(v) != 0 }
    fun <T : Comparable<T>> gt(v: T) = Validator<T> { t -> t != null && t > v }
    fun <T : Comparable<T>> gte(v: T) = Validator<T> { t -> t != null && t >= v }
    fun <T : Comparable<T>> lt(v: T) = Validator<T> { t -> t != null && t < v }
    fun <T : Comparable<T>> lte(v: T) = Validator<T> { t -> t != null && t <= v }
}
