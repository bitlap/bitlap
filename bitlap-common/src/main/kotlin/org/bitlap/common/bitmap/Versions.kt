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
package org.bitlap.common.bitmap

/**
 * Bitmap's serializer versions
 */
object Versions {
    const val RBM_VERSION_V1: Int = 1001
    const val BBM_VERSION_V1: Int = 2001
    const val CBM_VERSION_V1: Int = 3001

    // 64 bit
    const val RBM64_VERSION_V1: Int = 6001
}
