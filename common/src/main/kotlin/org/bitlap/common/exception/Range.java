package org.bitlap.common.exception;

/*
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.comparator.CompareUtil;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import org.bitlap.common.utils.PreConditions;
import org.jetbrains.annotations.Nullable;

/**
 * A range (or "interval") defines the <i>boundaries</i> around a contiguous span of values of some
 * {@code Comparable} type; for example, "integers from 1 to 100 inclusive." Note that it is not
 * possible to <i>iterate</i> over these contained values.
 *
 * <h3>Types of ranges</h3>
 *
 * <p>Each end of the range may be bounded or unbounded. If bounded, there is an associated
 * <i>endpoint</i> value, and the range is considered to be either <i>open</i> (does not include the
 * endpoint) or <i>closed</i> (includes the endpoint) on that side. With three possibilities on each
 * side, this yields nine basic types of ranges, enumerated below. (Notation: a square bracket
 * ({@code [ ]}) indicates that the range is closed on that side; a parenthesis ({@code ( )}) means
 * it is either open or unbounded. The construct {@code {x | statement}} is read "the set of all
 * <i>x</i> such that <i>statement</i>.")
 *
 * <blockquote>
 *
 * <table>
 * <caption>Range Types</caption>
 * <tr><th>Notation        <th>Definition               <th>Factory method
 * <tr><td>{@code (a..b)}  <td>{@code {x | a < x < b}}  <td>{@link Range#open open}
 * <tr><td>{@code [a..b]}  <td>{@code {x | a <= x <= b}}<td>{@link Range#closed closed}
 * <tr><td>{@code (a..b]}  <td>{@code {x | a < x <= b}} <td>{@link Range#openClosed openClosed}
 * <tr><td>{@code [a..b)}  <td>{@code {x | a <= x < b}} <td>{@link Range#closedOpen closedOpen}
 * <tr><td>{@code (a..+∞)} <td>{@code {x | x > a}}      <td>{@link Range#greaterThan greaterThan}
 * <tr><td>{@code [a..+∞)} <td>{@code {x | x >= a}}     <td>{@link Range#atLeast atLeast}
 * <tr><td>{@code (-∞..b)} <td>{@code {x | x < b}}      <td>{@link Range#lessThan lessThan}
 * <tr><td>{@code (-∞..b]} <td>{@code {x | x <= b}}     <td>{@link Range#atMost atMost}
 * <tr><td>{@code (-∞..+∞)}<td>{@code {x}}              <td>{@link Range#all all}
 * </table>
 *
 * </blockquote>
 *
 * <p>When both endpoints exist, the upper endpoint may not be less than the lower. The endpoints
 * may be equal only if at least one of the bounds is closed:
 *
 * <ul>
 *   <li>{@code [a..a]} : a singleton range
 *   <li>{@code [a..a); (a..a]} : {@linkplain #isEmpty empty} ranges; also valid
 *   <li>{@code (a..a)} : <b>invalid</b>; an exception will be thrown
 * </ul>
 *
 * <h3>Warnings</h3>
 *
 * <ul>
 *   <li>Use immutable value types only, if at all possible. If you must use a mutable type, <b>do
 *       not</b> allow the endpoint instances to mutate after the range is created!
 *   <li>Your value type's comparison method should be {@linkplain Comparable consistent with
 *       equals} if at all possible. Otherwise, be aware that concepts used throughout this
 *       documentation such as "equal", "same", "unique" and so on actually refer to whether {@link
 *       Comparable#compareTo compareTo} returns zero, not whether {@link Object#equals equals}
 *       returns {@code true}.
 *   <li>A class which implements {@code Comparable<UnrelatedType>} is very broken, and will cause
 *       undefined horrible things to happen in {@code Range}. For now, the Range API does not
 *       prevent its use, because this would also rule out all ungenerified (pre-JDK1.5) data types.
 *       <b>This may change in the future.</b>
 * </ul>
 *
 * <h3>Other notes</h3>
 *
 * <ul>
 *   <li>All ranges are shallow-immutable.
 *   <li>Instances of this type are obtained using the static factory methods in this class.
 *   <li>Ranges are <i>convex</i>: whenever two values are contained, all values in between them
 *       must also be contained. More formally, for any {@code c1 <= c2 <= c3} of type {@code C},
 *       {@code r.contains(c1) && r.contains(c3)} implies {@code r.contains(c2)}). This means that a
 *       {@code Range<Integer>} can never be used to represent, say, "all <i>prime</i> numbers from
 *       1 to 100."
 *   <li>When evaluated as a {@link Predicate}, a range yields the same result as invoking {@link
 *       #contains}.
 *   <li>Terminology note: a range {@code a} is said to be the <i>maximal</i> range having property
 *       <i>P</i> if, for all ranges {@code b} also having property <i>P</i>, {@code a.encloses(b)}.
 *       Likewise, {@code a} is <i>minimal</i> when {@code b.encloses(a)} for all {@code b} having
 *       property <i>P</i>. See, for example, the definition of {@link #intersection intersection}.
 * </ul>
 *
 * <h3>Further reading</h3>
 *
 * <p>See the Guava User Guide article on <a
 * href="https://github.com/google/guava/wiki/RangesExplained">{@code Range}</a>.
 *
 * @author Kevin Bourrillion
 * @author Gregory Kick
 * @since 10.0
 */
@SuppressWarnings("rawtypes")
public class Range<C extends Comparable> implements Serializable {

  static <C extends Comparable<?>> Range<C> create(Cut<C> lowerBound, Cut<C> upperBound) {
    return new Range<C>(lowerBound, upperBound);
  }

  /**
   * Returns a range that contains all values strictly greater than {@code lower} and strictly less
   * than {@code upper}.
   *
   * @throws IllegalArgumentException if {@code lower} is greater than <i>or equal to</i> {@code
   *     upper}
   * @throws ClassCastException if {@code lower} and {@code upper} are not mutually comparable
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> open(C lower, C upper) {
    return create(Cut.aboveValue(lower), Cut.belowValue(upper));
  }

  /**
   * Returns a range that contains all values greater than or equal to {@code lower} and less than
   * or equal to {@code upper}.
   *
   * @throws IllegalArgumentException if {@code lower} is greater than {@code upper}
   * @throws ClassCastException if {@code lower} and {@code upper} are not mutually comparable
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> closed(C lower, C upper) {
    return create(Cut.belowValue(lower), Cut.aboveValue(upper));
  }

  /**
   * Returns a range that contains all values greater than or equal to {@code lower} and strictly
   * less than {@code upper}.
   *
   * @throws IllegalArgumentException if {@code lower} is greater than {@code upper}
   * @throws ClassCastException if {@code lower} and {@code upper} are not mutually comparable
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> closedOpen(C lower, C upper) {
    return create(Cut.belowValue(lower), Cut.belowValue(upper));
  }

  /**
   * Returns a range that contains all values strictly greater than {@code lower} and less than or
   * equal to {@code upper}.
   *
   * @throws IllegalArgumentException if {@code lower} is greater than {@code upper}
   * @throws ClassCastException if {@code lower} and {@code upper} are not mutually comparable
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> openClosed(C lower, C upper) {
    return create(Cut.aboveValue(lower), Cut.aboveValue(upper));
  }

  /**
   * Returns a range that contains any value from {@code lower} to {@code upper}, where each
   * endpoint may be either inclusive (closed) or exclusive (open).
   *
   * @throws IllegalArgumentException if {@code lower} is greater than {@code upper}
   * @throws ClassCastException if {@code lower} and {@code upper} are not mutually comparable
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> range(
      C lower, BoundType lowerType, C upper, BoundType upperType) {
    PreConditions.checkNotNull(lowerType);
    PreConditions.checkNotNull(upperType);

    Cut<C> lowerBound =
        (lowerType == BoundType.OPEN) ? Cut.aboveValue(lower) : Cut.belowValue(lower);
    Cut<C> upperBound =
        (upperType == BoundType.OPEN) ? Cut.belowValue(upper) : Cut.aboveValue(upper);
    return create(lowerBound, upperBound);
  }

  /**
   * Returns a range that contains all values strictly less than {@code endpoint}.
   *
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> lessThan(C endpoint) {
    return create(Cut.<C>belowAll(), Cut.belowValue(endpoint));
  }

  /**
   * Returns a range that contains all values less than or equal to {@code endpoint}.
   *
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> atMost(C endpoint) {
    return create(Cut.<C>belowAll(), Cut.aboveValue(endpoint));
  }

  /**
   * Returns a range with no lower bound up to the given endpoint, which may be either inclusive
   * (closed) or exclusive (open).
   *
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> upTo(C endpoint, BoundType boundType) {
    switch (boundType) {
      case OPEN:
        return lessThan(endpoint);
      case CLOSED:
        return atMost(endpoint);
      default:
        throw new AssertionError();
    }
  }

  /**
   * Returns a range that contains all values strictly greater than {@code endpoint}.
   *
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> greaterThan(C endpoint) {
    return create(Cut.aboveValue(endpoint), Cut.<C>aboveAll());
  }

  /**
   * Returns a range that contains all values greater than or equal to {@code endpoint}.
   *
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> atLeast(C endpoint) {
    return create(Cut.belowValue(endpoint), Cut.<C>aboveAll());
  }

  /**
   * Returns a range from the given endpoint, which may be either inclusive (closed) or exclusive
   * (open), with no upper bound.
   *
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> downTo(C endpoint, BoundType boundType) {
    switch (boundType) {
      case OPEN:
        return greaterThan(endpoint);
      case CLOSED:
        return atLeast(endpoint);
      default:
        throw new AssertionError();
    }
  }

  private static final Range<Comparable> ALL = new Range<>(Cut.belowAll(), Cut.aboveAll());

  /**
   * Returns a range that contains every value of type {@code C}.
   *
   * @since 14.0
   */
  @SuppressWarnings("unchecked")
  public static <C extends Comparable<?>> Range<C> all() {
    return (Range) ALL;
  }

  /**
   * Returns a range that {@linkplain Range#contains(Comparable) contains} only the given value. The
   * returned range is {@linkplain BoundType#CLOSED closed} on both ends.
   *
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> singleton(C value) {
    return closed(value, value);
  }

  /**
   * Returns the minimal range that {@linkplain Range#contains(Comparable) contains} all of the
   * given values. The returned range is {@linkplain BoundType#CLOSED closed} on both ends.
   *
   * @throws ClassCastException if the values are not mutually comparable
   * @throws NoSuchElementException if {@code values} is empty
   * @throws NullPointerException if any of {@code values} is null
   * @since 14.0
   */
  public static <C extends Comparable<?>> Range<C> encloseAll(Iterable<C> values) {
    PreConditions.checkNotNull(values);
    if (values instanceof SortedSet) {
      SortedSet<? extends C> set = (SortedSet<C>) values;
      Comparator<?> comparator = set.comparator();
      if (comparator == null) {
        return closed(set.first(), set.last());
      }
    }
    Iterator<C> valueIterator = values.iterator();
    C min = PreConditions.checkNotNull(valueIterator.next());
    C max = min;
    while (valueIterator.hasNext()) {
      C value = PreConditions.checkNotNull(valueIterator.next());
      min = CompareUtil.<Comparable>compare(min, value) <= 0 ? min : value;
      max = CompareUtil.<Comparable>compare(max, value) >= 0 ? max : value;
    }
    return closed(min, max);
  }

  final Cut<C> lowerBound;
  final Cut<C> upperBound;

  private Range(Cut<C> lowerBound, Cut<C> upperBound) {
    this.lowerBound = PreConditions.checkNotNull(lowerBound);
    this.upperBound = PreConditions.checkNotNull(upperBound);
    if (lowerBound.compareTo(upperBound) > 0
        || lowerBound == Cut.<C>aboveAll()
        || upperBound == Cut.<C>belowAll()) {
      throw new IllegalArgumentException("Invalid range: " + toString(lowerBound, upperBound));
    }
  }

  /** Returns {@code true} if this range has a lower endpoint. */
  public boolean hasLowerBound() {
    return lowerBound != Cut.belowAll();
  }

  /**
   * Returns the lower endpoint of this range.
   *
   * @throws IllegalStateException if this range is unbounded below (that is, {@link
   *     #hasLowerBound()} returns {@code false})
   */
  public C lowerEndpoint() {
    return lowerBound.endpoint();
  }

  /**
   * Returns the type of this range's lower bound: {@link BoundType#CLOSED} if the range includes
   * its lower endpoint, {@link BoundType#OPEN} if it does not.
   *
   * @throws IllegalStateException if this range is unbounded below (that is, {@link
   *     #hasLowerBound()} returns {@code false})
   */
  public BoundType lowerBoundType() {
    return lowerBound.typeAsLowerBound();
  }

  /** Returns {@code true} if this range has an upper endpoint. */
  public boolean hasUpperBound() {
    return upperBound != Cut.aboveAll();
  }

  /**
   * Returns the upper endpoint of this range.
   *
   * @throws IllegalStateException if this range is unbounded above (that is, {@link
   *     #hasUpperBound()} returns {@code false})
   */
  public C upperEndpoint() {
    return upperBound.endpoint();
  }

  /**
   * Returns the type of this range's upper bound: {@link BoundType#CLOSED} if the range includes
   * its upper endpoint, {@link BoundType#OPEN} if it does not.
   *
   * @throws IllegalStateException if this range is unbounded above (that is, {@link
   *     #hasUpperBound()} returns {@code false})
   */
  public BoundType upperBoundType() {
    return upperBound.typeAsUpperBound();
  }

  /**
   * Returns {@code true} if this range is of the form {@code [v..v)} or {@code (v..v]}. (This does
   * not encompass ranges of the form {@code (v..v)}, because such ranges are <i>invalid</i> and
   * can't be constructed at all.)
   *
   * <p>Note that certain discrete ranges such as the integer range {@code (3..4)} are <b>not</b>
   * considered empty, even though they contain no actual values.
   */
  public boolean isEmpty() {
    return lowerBound.equals(upperBound);
  }

  /**
   * Returns {@code true} if {@code value} is within the bounds of this range. For example, on the
   * range {@code [0..2)}, {@code contains(1)} returns {@code true}, while {@code contains(2)}
   * returns {@code false}.
   */
  public boolean contains(C value) {
    PreConditions.checkNotNull(value);
    // let this throw CCE if there is some trickery going on
    return lowerBound.isLessThan(value) && !upperBound.isLessThan(value);
  }

  /**
   * Returns {@code true} if every element in {@code values} is {@linkplain #contains contained} in
   * this range.
   */
  public boolean containsAll(Iterable<? extends C> values) {
    if (CollectionUtil.isEmpty(values)) {
      return true;
    }

    // this optimizes testing equality of two range-backed sets
    if (values instanceof SortedSet) {
      SortedSet<? extends C> set = (SortedSet<C>) values;
      Comparator<?> comparator = set.comparator();
      if (comparator == null) {
        return contains(set.first()) && contains(set.last());
      }
    }

    for (C value : values) {
      if (!contains(value)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns {@code true} if the bounds of {@code other} do not extend outside the bounds of this
   * range. Examples:
   *
   * <ul>
   *   <li>{@code [3..6]} encloses {@code [4..5]}
   *   <li>{@code (3..6)} encloses {@code (3..6)}
   *   <li>{@code [3..6]} encloses {@code [4..4)} (even though the latter is empty)
   *   <li>{@code (3..6]} does not enclose {@code [3..6]}
   *   <li>{@code [4..5]} does not enclose {@code (3..6)} (even though it contains every value
   *       contained by the latter range)
   *   <li>{@code [3..6]} does not enclose {@code (1..1]} (even though it contains every value
   *       contained by the latter range)
   * </ul>
   *
   * <p>Note that if {@code a.encloses(b)}, then {@code b.contains(v)} implies {@code
   * a.contains(v)}, but as the last two examples illustrate, the converse is not always true.
   *
   * <p>Being reflexive, antisymmetric and transitive, the {@code encloses} relation defines a
   * <i>partial order</i> over ranges. There exists a unique {@linkplain Range#all maximal} range
   * according to this relation, and also numerous {@linkplain #isEmpty minimal} ranges. Enclosure
   * also implies {@linkplain #isConnected connectedness}.
   */
  public boolean encloses(Range<C> other) {
    return lowerBound.compareTo(other.lowerBound) <= 0
        && upperBound.compareTo(other.upperBound) >= 0;
  }

  /**
   * Returns {@code true} if there exists a (possibly empty) range which is {@linkplain #encloses
   * enclosed} by both this range and {@code other}.
   *
   * <p>For example,
   *
   * <ul>
   *   <li>{@code [2, 4)} and {@code [5, 7)} are not connected
   *   <li>{@code [2, 4)} and {@code [3, 5)} are connected, because both enclose {@code [3, 4)}
   *   <li>{@code [2, 4)} and {@code [4, 6)} are connected, because both enclose the empty range
   *       {@code [4, 4)}
   * </ul>
   *
   * <p>Note that this range and {@code other} have a well-defined {@linkplain #span union} and
   * {@linkplain #intersection intersection} (as a single, possibly-empty range) if and only if this
   * method returns {@code true}.
   *
   * <p>The connectedness relation is both reflexive and symmetric, but does not form an {@linkplain
   * Equivalence equivalence relation} as it is not transitive.
   *
   * <p>Note that certain discrete ranges are not considered connected, even though there are no
   * elements "between them." For example, {@code [3, 5]} is not considered connected to {@code [6,
   * 10]}.
   */
  public boolean isConnected(Range<C> other) {
    return lowerBound.compareTo(other.upperBound) <= 0
        && other.lowerBound.compareTo(upperBound) <= 0;
  }

  /**
   * Returns the maximal range {@linkplain #encloses enclosed} by both this range and {@code
   * connectedRange}, if such a range exists.
   *
   * <p>For example, the intersection of {@code [1..5]} and {@code (3..7)} is {@code (3..5]}. The
   * resulting range may be empty; for example, {@code [1..5)} intersected with {@code [5..7)}
   * yields the empty range {@code [5..5)}.
   *
   * <p>The intersection exists if and only if the two ranges are {@linkplain #isConnected
   * connected}.
   *
   * <p>The intersection operation is commutative, associative and idempotent, and its identity
   * element is {@link Range#all}).
   *
   * @throws IllegalArgumentException if {@code isConnected(connectedRange)} is {@code false}
   */
  public Range<C> intersection(Range<C> connectedRange) {
    int lowerCmp = lowerBound.compareTo(connectedRange.lowerBound);
    int upperCmp = upperBound.compareTo(connectedRange.upperBound);
    if (lowerCmp >= 0 && upperCmp <= 0) {
      return this;
    } else if (lowerCmp <= 0 && upperCmp >= 0) {
      return connectedRange;
    } else {
      Cut<C> newLower = (lowerCmp >= 0) ? lowerBound : connectedRange.lowerBound;
      Cut<C> newUpper = (upperCmp <= 0) ? upperBound : connectedRange.upperBound;
      return create(newLower, newUpper);
    }
  }

  /**
   * Returns the maximal range lying between this range and {@code otherRange}, if such a range
   * exists. The resulting range may be empty if the two ranges are adjacent but non-overlapping.
   *
   * <p>For example, the gap of {@code [1..5]} and {@code (7..10)} is {@code (5..7]}. The resulting
   * range may be empty; for example, the gap between {@code [1..5)} {@code [5..7)} yields the empty
   * range {@code [5..5)}.
   *
   * <p>The gap exists if and only if the two ranges are either disconnected or immediately adjacent
   * (any intersection must be an empty range).
   *
   * <p>The gap operation is commutative.
   *
   * @throws IllegalArgumentException if this range and {@code otherRange} have a nonempty
   *     intersection
   * @since 27.0
   */
  public Range<C> gap(Range<C> otherRange) {
    /*
     * For an explanation of the basic principle behind this check, see
     * https://stackoverflow.com/a/35754308/28465
     *
     * In that explanation's notation, our `overlap` check would be `x1 < y2 && y1 < x2`. We've
     * flipped one part of the check so that we're using "less than" in both cases (rather than a
     * mix of "less than" and "greater than"). We've also switched to "strictly less than" rather
     * than "less than or equal to" because of *handwave* the difference between "endpoints of
     * inclusive ranges" and "Cuts."
     */
    if (lowerBound.compareTo(otherRange.upperBound) < 0
        && otherRange.lowerBound.compareTo(upperBound) < 0) {
      throw new IllegalArgumentException(
          "Ranges have a nonempty intersection: " + this + ", " + otherRange);
    }

    boolean isThisFirst = this.lowerBound.compareTo(otherRange.lowerBound) < 0;
    Range<C> firstRange = isThisFirst ? this : otherRange;
    Range<C> secondRange = isThisFirst ? otherRange : this;
    return create(firstRange.upperBound, secondRange.lowerBound);
  }

  /**
   * Returns the minimal range that {@linkplain #encloses encloses} both this range and {@code
   * other}. For example, the span of {@code [1..3]} and {@code (5..7)} is {@code [1..7)}.
   *
   * <p><i>If</i> the input ranges are {@linkplain #isConnected connected}, the returned range can
   * also be called their <i>union</i>. If they are not, note that the span might contain values
   * that are not contained in either input range.
   *
   * <p>Like {@link #intersection(Range) intersection}, this operation is commutative, associative
   * and idempotent. Unlike it, it is always well-defined for any two input ranges.
   */
  public Range<C> span(Range<C> other) {
    int lowerCmp = lowerBound.compareTo(other.lowerBound);
    int upperCmp = upperBound.compareTo(other.upperBound);
    if (lowerCmp <= 0 && upperCmp >= 0) {
      return this;
    } else if (lowerCmp >= 0 && upperCmp <= 0) {
      return other;
    } else {
      Cut<C> newLower = (lowerCmp <= 0) ? lowerBound : other.lowerBound;
      Cut<C> newUpper = (upperCmp >= 0) ? upperBound : other.upperBound;
      return create(newLower, newUpper);
    }
  }


  /**
   * Returns {@code true} if {@code object} is a range having the same endpoints and bound types as
   * this range. Note that discrete ranges such as {@code (1..4)} and {@code [2..3]} are <b>not</b>
   * equal to one another, despite the fact that they each contain precisely the same set of values.
   * Similarly, empty ranges are not equal unless they have exactly the same representation, so
   * {@code [3..3)}, {@code (3..3]}, {@code (4..4]} are all unequal.
   */
  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof Range) {
      Range<?> other = (Range<?>) object;
      return lowerBound.equals(other.lowerBound) && upperBound.equals(other.upperBound);
    }
    return false;
  }

  /** Returns a hash code for this range. */
  @Override
  public int hashCode() {
    return lowerBound.hashCode() * 31 + upperBound.hashCode();
  }

  /**
   * Returns a string representation of this range, such as {@code "[3..5)"} (other examples are
   * listed in the class documentation).
   */
  @Override
  public String toString() {
    return toString(lowerBound, upperBound);
  }

  private static String toString(Cut<?> lowerBound, Cut<?> upperBound) {
    StringBuilder sb = new StringBuilder(16);
    lowerBound.describeAsLowerBound(sb);
    sb.append("..");
    upperBound.describeAsUpperBound(sb);
    return sb.toString();
  }

  Object readResolve() {
    if (this.equals(ALL)) {
      return all();
    } else {
      return this;
    }
  }

  @SuppressWarnings("unchecked") // this method may throw CCE
  static int compareOrThrow(Comparable left, Comparable right) {
    return left.compareTo(right);
  }

  private static final long serialVersionUID = 0;
}

enum BoundType {
    /** The endpoint value <i>is not</i> considered part of the set ("exclusive"). */
    OPEN(false),
    CLOSED(true);

    final boolean inclusive;

    BoundType(boolean inclusive) {
        this.inclusive = inclusive;
    }

    /** Returns the bound type corresponding to a boolean value for inclusivity. */
    static BoundType forBoolean(boolean inclusive) {
        return inclusive ? CLOSED : OPEN;
    }

    BoundType flip() {
        return forBoolean(!inclusive);
    }
}

/**
 * Implementation detail for the internal structure of {@link Range} instances. Represents a unique
 * way of "cutting" a "number line" (actually of instances of type {@code C}, not necessarily
 * "numbers") into two sections; this can be done below a certain value, above a certain value,
 * below all values or above all values. With this object defined in this way, an interval can
 * always be represented by a pair of {@code Cut} instances.
 *
 * @author Kevin Bourrillion
 */
abstract class Cut<C extends Comparable> implements Comparable<Cut<C>>, Serializable {
  final @Nullable C endpoint;

  Cut(@Nullable C endpoint) {
    this.endpoint = endpoint;
  }

  abstract boolean isLessThan(C value);

  abstract BoundType typeAsLowerBound();

  abstract BoundType typeAsUpperBound();

  abstract void describeAsLowerBound(StringBuilder sb);

  abstract void describeAsUpperBound(StringBuilder sb);

  // note: overridden by {BELOW,ABOVE}_ALL
  @Override
  public int compareTo(Cut<C> that) {
    if (that == belowAll()) {
      return 1;
    }
    if (that == aboveAll()) {
      return -1;
    }
    int result = Range.compareOrThrow(endpoint, that.endpoint);
    if (result != 0) {
      return result;
    }
    // same value. below comes before above
    return Boolean.compare(this instanceof AboveValue, that instanceof AboveValue);
  }

  C endpoint() {
    return endpoint;
  }

  @SuppressWarnings("unchecked") // catching CCE
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Cut) {
      // It might not really be a Cut<C>, but we'll catch a CCE if it's not
      Cut<C> that = (Cut<C>) obj;
      try {
        int compareResult = compareTo(that);
        return compareResult == 0;
      } catch (ClassCastException ignored) {
      }
    }
    return false;
  }

  // Prevent "missing hashCode" warning by explicitly forcing subclasses implement it
  @Override
  public abstract int hashCode();

  /*
   * The implementation neither produces nor consumes any non-null instance of type C, so
   * casting the type parameter is safe.
   */
  @SuppressWarnings("unchecked")
  static <C extends Comparable> Cut<C> belowAll() {
    return (Cut<C>) BelowAll.INSTANCE;
  }

  private static final long serialVersionUID = 0;

  private static final class BelowAll extends Cut<Comparable<?>> {
    private static final Cut.BelowAll INSTANCE = new Cut.BelowAll();

    private BelowAll() {
      super(null);
    }

    @Override
    Comparable<?> endpoint() {
      throw new IllegalStateException("range unbounded on this side");
    }

    @Override
    boolean isLessThan(Comparable<?> value) {
      return true;
    }

    @Override
    BoundType typeAsLowerBound() {
      throw new IllegalStateException();
    }

    @Override
    BoundType typeAsUpperBound() {
      throw new AssertionError("this statement should be unreachable");
    }

    @Override
    void describeAsLowerBound(StringBuilder sb) {
      sb.append("(-\u221e");
    }

    @Override
    void describeAsUpperBound(StringBuilder sb) {
      throw new AssertionError();
    }

    @Override
    public int compareTo(Cut<Comparable<?>> o) {
      return (o == this) ? 0 : -1;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }

    @Override
    public String toString() {
      return "-\u221e";
    }

    private Object readResolve() {
      return INSTANCE;
    }

    private static final long serialVersionUID = 0;
  }

  /*
   * The implementation neither produces nor consumes any non-null instance of
   * type C, so casting the type parameter is safe.
   */
  @SuppressWarnings("unchecked")
  static <C extends Comparable> Cut<C> aboveAll() {
    return (Cut<C>) AboveAll.INSTANCE;
  }

  private static final class AboveAll extends Cut<Comparable<?>> {
    private static final Cut.AboveAll INSTANCE = new Cut.AboveAll();

    private AboveAll() {
      super(null);
    }

    @Override
    Comparable<?> endpoint() {
      throw new IllegalStateException("range unbounded on this side");
    }

    @Override
    boolean isLessThan(Comparable<?> value) {
      return false;
    }

    @Override
    BoundType typeAsLowerBound() {
      throw new AssertionError("this statement should be unreachable");
    }

    @Override
    BoundType typeAsUpperBound() {
      throw new IllegalStateException();
    }

      @Override
      void describeAsLowerBound(StringBuilder sb) {
          throw new AssertionError();
      }

      @Override
      void describeAsUpperBound(StringBuilder sb) {
          sb.append("+\u221e)");
      }

    @Override
    public int compareTo(Cut<Comparable<?>> o) {
      return (o == this) ? 0 : 1;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }

    @Override
    public String toString() {
      return "+\u221e";
    }

    private Object readResolve() {
      return INSTANCE;
    }

    private static final long serialVersionUID = 0;
  }

  static <C extends Comparable> Cut<C> belowValue(C endpoint) {
    return new BelowValue<C>(endpoint);
  }

  private static final class BelowValue<C extends Comparable> extends Cut<C> {
    BelowValue(C endpoint) {
      super(PreConditions.checkNotNull(endpoint));
    }

    @Override
    boolean isLessThan(C value) {
      return Range.compareOrThrow(endpoint, value) <= 0;
    }

    @Override
    BoundType typeAsLowerBound() {
      return BoundType.CLOSED;
    }

    @Override
    BoundType typeAsUpperBound() {
      return BoundType.OPEN;
    }

    @Override
    void describeAsLowerBound(StringBuilder sb) {
      sb.append('[').append(endpoint);
    }

    @Override
    void describeAsUpperBound(StringBuilder sb) {
      sb.append(endpoint).append(')');
    }

    @Override
    public int hashCode() {
      return endpoint.hashCode();
    }

    @Override
    public String toString() {
      return "\\" + endpoint + "/";
    }

    private static final long serialVersionUID = 0;
  }

  static <C extends Comparable> Cut<C> aboveValue(C endpoint) {
    return new AboveValue<C>(endpoint);
  }

  private static final class AboveValue<C extends Comparable> extends Cut<C> {
    AboveValue(C endpoint) {
      super(PreConditions.checkNotNull(endpoint));
    }

    @Override
    boolean isLessThan(C value) {
      return Range.compareOrThrow(endpoint, value) < 0;
    }

    @Override
    BoundType typeAsLowerBound() {
      return BoundType.OPEN;
    }

    @Override
    BoundType typeAsUpperBound() {
      return BoundType.CLOSED;
    }

    @Override
    void describeAsLowerBound(StringBuilder sb) {
      sb.append('(').append(endpoint);
    }

    @Override
    void describeAsUpperBound(StringBuilder sb) {
      sb.append(endpoint).append(']');
    }

    @Override
    public int hashCode() {
      return ~endpoint.hashCode();
    }

    @Override
    public String toString() {
      return "/" + endpoint + "\\";
    }

    private static final long serialVersionUID = 0;
  }
}
