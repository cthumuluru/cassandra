/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

public class IntervalTree2<C extends Comparable<C>, D, I extends Interval<C, D>> implements Iterable<I> {
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static final IntervalTree2 EMPTY_TREE = new IntervalTree2(Collections.emptyList(), null);

	private final Comparator<C> comparator;
	private final Comparator<C> comparatorWithFallback;

	private final List<I> minOrderedIntervals;
	private final List<I> maxOrderedIntervals;

	private C low = null;
	private C high = null;

	final Comparator<I> minOrdering;
	final Comparator<I> maxOrdering;

	public IntervalTree2(Collection<I> intervals, Comparator<C> comparator) {
		this.comparator = comparator;
		this.comparatorWithFallback = Optional.ofNullable(comparator).orElse((c1, c2) -> c1.compareTo(c2));
		this.minOrdering = (i1, i2) -> comparatorWithFallback.compare(i1.min, i2.min);
		this.maxOrdering = (i1, i2) -> comparatorWithFallback.compare(i1.max, i2.max);

		minOrderedIntervals = Collections
				.unmodifiableList(intervals.stream().sorted(minOrdering).collect(Collectors.toList()));
		maxOrderedIntervals = Collections
				.unmodifiableList(intervals.stream().sorted(maxOrdering).collect(Collectors.toList()));

		if (!minOrderedIntervals.isEmpty()) {
			low = minOrderedIntervals.get(0).min;
			high = maxOrderedIntervals.get(0).max;
		}
	}

	@Override
	public Iterator<I> iterator() {
		return minOrderedIntervals.iterator();
	}

	public List<D> search(Interval<C, D> searchInterval) {
		if (minOrderedIntervals.isEmpty()) {
			return Collections.emptyList();
		}

		// encloses all intervals.
		if (searchInterval.min.compareTo(low) <= 0 && searchInterval.max.compareTo(high) >= 0) {
			return minOrderedIntervals.stream().map(i -> i.data).collect(Collectors.toList());
		}

		// TODO: REWORD
		// find the ceil min element index bigger than max index of search interval.
		int endPos = ceil(minOrderedIntervals, searchInterval);

		// TODO: REWORD
		// find the floor max element index smaller than the min index of search
		// interval.
		int startPos = floor(maxOrderedIntervals, searchInterval);

		// prefer the shorter sublist to traverse.
		if (maxOrderedIntervals.size() - startPos < endPos) {
			return maxOrderedIntervals.subList(startPos, maxOrderedIntervals.size()).stream()
					.filter(i -> overlaps(i, searchInterval)).map(i -> i.data).collect(Collectors.toList());
		} else {
			return minOrderedIntervals.subList(0, endPos).stream().filter(i -> overlaps(i, searchInterval))
					.map(i -> i.data).collect(Collectors.toList());
		}
	}

	private int ceil(List<I> orderedIntervals, Interval<C, D> searchInterval) {
		int endPos = Collections.binarySearch(orderedIntervals, searchInterval,
				(i1, i2) -> comparatorWithFallback.compare(i1.min, i2.max));
		endPos = (endPos < 0) ? Math.abs(endPos + 1) : endPos;

		// binary search given insert position or any position with the same value we
		// are searching for. in case of same value, find the position of the last
		// value.
		while (endPos < orderedIntervals.size() && orderedIntervals.get(endPos).min.equals(searchInterval.max)) {
			endPos++;
		}

		return endPos;
	}

	private int floor(List<I> orderedIntervals, Interval<C, D> searchInterval) {
		int startPos = Collections.binarySearch(orderedIntervals, searchInterval,
				(i1, i2) -> comparatorWithFallback.compare(i1.max, i2.min));
		startPos = (startPos < 0) ? Math.abs(startPos + 1) : startPos;

		// binary search given insert position or any position with the same value we
		// are searching for. in case of same value, find the position of the last
		// value.
		while (startPos > 0 && orderedIntervals.get(startPos).max.equals(searchInterval.min)) {
			startPos--;
		}

		return startPos;
	}

	private boolean overlaps(Interval<C, D> i1, Interval<C, D> i2) {
		return !((i1.min.compareTo(i2.max) > 0) || (i1.max.compareTo(i2.min) < 0));
	}

	public List<D> search(C point) {
		return search(Interval.<C, D>create(point, point, null));
	}

	public static <C extends Comparable<C>, D, I extends Interval<C, D>> IntervalTree2<C, D, I> build(
			Collection<I> intervals, Comparator<C> comparator) {
		if (intervals == null || intervals.isEmpty())
			return emptyTree();

		return new IntervalTree2<C, D, I>(intervals, comparator);
	}

	public static <C extends Comparable<C>, D, I extends Interval<C, D>> IntervalTree2<C, D, I> build(
			Collection<I> intervals) {
		if (intervals == null || intervals.isEmpty())
			return emptyTree();

		return new IntervalTree2<C, D, I>(intervals, null);
	}

	public static <C extends Comparable<C>, D, I extends Interval<C, D>> Serializer<C, D, I> serializer(
			ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, Constructor<I> constructor) {
		return new Serializer<>(pointSerializer, dataSerializer, constructor);
	}

	@SuppressWarnings("unchecked")
	public static <C extends Comparable<C>, D, I extends Interval<C, D>> IntervalTree2<C, D, I> emptyTree() {
		return (IntervalTree2<C, D, I>) EMPTY_TREE;
	}

	public Comparator<C> comparator() {
		return comparator;
	}

	public int intervalCount() {
		return minOrderedIntervals.size();
	}

	public boolean isEmpty() {
		return minOrderedIntervals.isEmpty();
	}

	public C max() {
		if (minOrderedIntervals.isEmpty()) {
			throw new IllegalStateException();
		}

		return high;
	}

	public C min() {
		if (minOrderedIntervals.isEmpty()) {
			throw new IllegalStateException();
		}

		return low;
	}

	@Override
	public String toString() {
		return "<" + Joiner.on(", ").join(this) + ">";
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IntervalTree2)) {
			return false;
		}

		@SuppressWarnings("rawtypes")
		IntervalTree2 that = (IntervalTree2) o;

		return Iterators.elementsEqual(iterator(), that.iterator());
	}

	@Override
	public final int hashCode() {
		int result = comparator.hashCode();
		for (Interval<C, D> interval : this)
			result = 31 * result + interval.hashCode();
		return result;
	}

	public static class Serializer<C extends Comparable<C>, D, I extends Interval<C, D>>
			implements IVersionedSerializer<IntervalTree2<C, D, I>> {
		private final ISerializer<C> pointSerializer;
		private final ISerializer<D> dataSerializer;
		private final Constructor<I> constructor;

		private Serializer(ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, Constructor<I> constructor) {
			this.pointSerializer = pointSerializer;
			this.dataSerializer = dataSerializer;
			this.constructor = constructor;
		}

		public void serialize(IntervalTree2<C, D, I> it, DataOutputPlus out, int version) throws IOException {
			out.writeInt(it.minOrderedIntervals.size());
			for (Interval<C, D> interval : it) {
				pointSerializer.serialize(interval.min, out);
				pointSerializer.serialize(interval.max, out);
				dataSerializer.serialize(interval.data, out);
			}
		}

		/**
		 * Deserialize an IntervalTree whose keys use the natural ordering. Use
		 * deserialize(DataInput, int, Comparator) instead if the interval tree is to
		 * use a custom comparator, as the comparator is *not* serialized.
		 */
		public IntervalTree2<C, D, I> deserialize(DataInput in, int version) throws IOException {
			return deserialize(in, version, null);
		}

		public IntervalTree2<C, D, I> deserialize(DataInput in, int version, Comparator<C> comparator)
				throws IOException {
			try {
				int count = in.readInt();
				List<Interval<C, D>> intervals = new ArrayList<Interval<C, D>>(count);
				for (int i = 0; i < count; i++) {
					C min = pointSerializer.deserialize(in);
					C max = pointSerializer.deserialize(in);
					D data = dataSerializer.deserialize(in);
					intervals.add(constructor.newInstance(min, max, data));
				}
				return new IntervalTree2(intervals, comparator);
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		public long serializedSize(IntervalTree2<C, D, I> it, TypeSizes typeSizes, int version) {
			long size = typeSizes.sizeof(0);
			for (Interval<C, D> interval : it) {
				size += pointSerializer.serializedSize(interval.min, typeSizes);
				size += pointSerializer.serializedSize(interval.max, typeSizes);
				size += dataSerializer.serializedSize(interval.data, typeSizes);
			}
			return size;
		}

		public long serializedSize(IntervalTree2<C, D, I> it, int version) {
			return serializedSize(it, TypeSizes.NATIVE, version);
		}
	}
}
