package org.bitlap.common.data

/**
 * Desc: Common entity in [Event]
 *
 * [key]: DEVICE, USER, etc.
 * [id]: identify of this entity, current only support int type
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/8
 */
data class Entity(val id: Int, val key: String = "ENTITY")
