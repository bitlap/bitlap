/* Copyright (c) 2022 bitlap.org */
package org.bitlap.testkit.csv

import shapeless._

import scala.collection.immutable.{ :: => Cons }
import scala.util.{ Failure, Success, Try }

/**
 * Csv encoder and decoder implement by shapeless.
 *
 * @author 梦境迷离
 * @since 2022/04/27
 * @version 1.0
 */
trait CsvConverter[T] {
  def from(s: String): Try[T]
  def to(t: T): String
}

object CsvConverter {
  def apply[T](implicit st: => CsvConverter[T]): CsvConverter[T] = st

  def fail(s: String): Failure[Nothing] = Failure(CsvException(s))

  // Primitives
  implicit def stringCSVConverter: CsvConverter[String] = new CsvConverter[String] {
    def from(s: String): Try[String] = Success(s)
    def to(s: String): String = s
  }

  implicit def intCsvConverter: CsvConverter[Int] = new CsvConverter[Int] {
    def from(s: String): Try[Int] = Try(s.toInt)
    def to(i: Int): String = i.toString
  }

  implicit def longCsvConverter: CsvConverter[Long] = new CsvConverter[Long] {
    def from(s: String): Try[Long] = Try(s.toLong)
    def to(i: Long): String = i.toString
  }

  implicit def doubleCsvConverter: CsvConverter[Double] = new CsvConverter[Double] {
    def from(s: String): Try[Double] = Try(s.toDouble)
    def to(i: Double): String = i.toString
  }

  def listCsvLinesConverter[A](l: List[String])(implicit ec: CsvConverter[A]): Try[List[A]] = l match {
    case Nil => Success(Nil)
    case Cons(s, ss) =>
      for {
        x <- ec.from(s)
        xs <- listCsvLinesConverter(ss)(ec)
      } yield Cons(x, xs)
  }

  implicit def listCsvConverter[A](implicit ec: CsvConverter[A]): CsvConverter[List[A]] = new CsvConverter[List[A]] {
    def from(s: String): Try[List[A]] = listCsvLinesConverter(s.split("\n").toList)(ec)
    def to(l: List[A]): String = l.map(ec.to).mkString("\n")
  }

  // HList
  implicit def deriveHNil: CsvConverter[HNil] =
    new CsvConverter[HNil] {
      def from(s: String): Try[HNil] = s match {
        case "" => Success(HNil)
        case s  => fail("Cannot convert '" ++ s ++ "' to HNil")
      }
      def to(n: HNil) = ""
    }

  implicit def deriveHCons[V, T <: HList](implicit
    scv: => CsvConverter[V],
    sct: => CsvConverter[T]
  ): CsvConverter[V :: T] =
    new CsvConverter[V :: T] {
      def from(s: String): Try[V :: T] = s.span(_ != ',') match {
        case (before, after) =>
          for {
            front <- scv.from(before)
            back <- sct.from(if (after.isEmpty) after else after.tail)
          } yield front :: back

        case _ => fail("Cannot convert '" ++ s ++ "' to HList")
      }

      def to(ft: V :: T): String =
        scv.to(ft.head) ++ "," ++ sct.to(ft.tail)
    }

  implicit def deriveHConsOption[V, T <: HList](implicit
    scv: => CsvConverter[V],
    sct: => CsvConverter[T]
  ): CsvConverter[Option[V] :: T] =
    new CsvConverter[Option[V] :: T] {
      def from(s: String): Try[Option[V] :: T] = s.span(_ != ',') match {
        case (before, after) =>
          (for {
            front <- scv.from(before)
            back <- sct.from(if (after.isEmpty) after else after.tail)
          } yield Some(front) :: back).orElse {
            sct.from(if (s.isEmpty) s else s.tail).map(None :: _)
          }

        case _ => fail("Cannot convert '" ++ s ++ "' to HList")
      }

      def to(ft: Option[V] :: T): String =
        ft.head.map(scv.to(_) ++ ",").getOrElse("") ++ sct.to(ft.tail)
    }

  // Anything with a Generic
  implicit def deriveClass[A, R](implicit gen: Generic.Aux[A, R], conv: CsvConverter[R]): CsvConverter[A] =
    new CsvConverter[A] {
      def from(s: String): Try[A] = conv.from(s).map(gen.from)
      def to(a: A): String = conv.to(gen.to(a))
    }
}
