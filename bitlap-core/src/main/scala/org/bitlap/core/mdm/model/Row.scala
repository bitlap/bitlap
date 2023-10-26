/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.model

import java.io.Serializable

import org.bitlap.core.mdm.format.DataType

/** common row for fetcher
 */
class Row(val data: Array[Any]) extends Serializable {

  def this(size: Int) = this(Array.fill[Any](size)(null))

  def set(idx: Int, value: Any): Unit = {
    this.data(idx) = value
  }

  def update(idx: Int, value: Any): Unit = {
    this.data(idx) = value
  }

  def get(idx: Int): Any = {
    this.data(idx)
  }

  def apply(idx: Int): Any = {
    this.data(idx)
  }

  def get(`type`: DataType): Any = {
    this.data(`type`.idx)
  }

  def apply(`type`: DataType): Any = {
    this.data(`type`.idx)
  }

  def getString(idx: Int): String = {
    Option(this(idx)).map(_.toString()).orNull
  }

  def getByIdxs(idxs: List[Int]): List[Any] = {
    idxs.map(i => this.data(i))
  }

  def getByTypes(types: List[DataType]): List[Any] = {
    types.map(t => this.data(t.idx))
  }

}
