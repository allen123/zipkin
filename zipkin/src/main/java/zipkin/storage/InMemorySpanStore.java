/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.DependencyLink;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.internal.CorrectForClockSkew;
import zipkin.internal.DependencyLinkSpan;
import zipkin.internal.DependencyLinker;
import zipkin.internal.MergeById;
import zipkin.internal.Nullable;
import zipkin.internal.Pair;

import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;
import static zipkin.internal.Util.UTF_8;
import static zipkin.internal.Util.sortedList;

public final class InMemorySpanStore implements SpanStore {
  private final Multimap<Pair<Long>, Span> traceIdToSpans = new LinkedListMultimap<Pair<Long>, Span>();
  private final Set<LongTriple> traceIdTimeStamps = new TreeSet<LongTriple>(VALUE_3_DESCENDING);
  private final Multimap<String, LongTriple> serviceToTraceIdTimeStamp =
      new SortedByValue3Descending<String>();
  private final Multimap<String, String> serviceToSpanNames = new LinkedHashSetMultimap<String, String>();
  volatile int acceptedSpanCount;

  final StorageAdapters.SpanConsumer spanConsumer = new StorageAdapters.SpanConsumer() {
    @Override public void accept(List<Span> spans) {
      for (Span span : spans) {
        Long timestamp = guessTimestamp(span);
        LongTriple traceIdTimeStamp = new LongTriple(span.traceIdHigh, span.traceId,
                timestamp == null ? Long.MIN_VALUE : timestamp);
        String spanName = span.name;
        synchronized (InMemorySpanStore.this) {
          traceIdTimeStamps.add(traceIdTimeStamp);
          traceIdToSpans.put(Pair.create(span.traceIdHigh, span.traceId), span);
          if (span.traceIdHigh != 0) {
            traceIdToSpans.put(Pair.create(0L, span.traceId), span);
          }
          acceptedSpanCount++;

          for (String serviceName : span.serviceNames()) {
            serviceToTraceIdTimeStamp.put(serviceName, traceIdTimeStamp);
            serviceToSpanNames.put(serviceName, spanName);
          }
        }
      }
    }

    @Override public String toString() {
      return "InMemorySpanConsumer";
    }
  };

  public synchronized List<Long> traceIds() {
    Set<Pair<Long>> input = traceIdToSpans.keySet();
    List<Long> result = new ArrayList<Long>(input.size());
    for (Pair<Long> pair : input) {
      result.add(pair._2);
    }
    return result;
  }

  synchronized void clear() {
    acceptedSpanCount = 0;
    traceIdToSpans.clear();
    serviceToTraceIdTimeStamp.clear();
  }

  @Override
  public synchronized List<List<Span>> getTraces(QueryRequest request) {
    Set<Pair<Long>> traceIds = traceIdsDescendingByTimestamp(request.serviceName);
    if (traceIds == null || traceIds.isEmpty()) return Collections.emptyList();

    Set<List<Span>> grouped = new LinkedHashSet<List<Span>>(traceIds.size());
    for (Pair<Long> traceId : traceIds) {
      List<Span> next = request.groupByTraceIdHigh
          ? getTrace(traceId._1, traceId._2)
          : getTrace(traceId._2);
      if (next != null && test(request, next)) {
        grouped.add(next);
      }
      if (grouped.size() == request.limit) {
        break;
      }
    }
    ArrayList<List<Span>> result = new ArrayList<List<Span>>(grouped);
    Collections.sort(result, TRACE_DESCENDING);
    return result;
  }

  Set<Pair<Long>> traceIdsDescendingByTimestamp(@Nullable String serviceName) {
    Collection<LongTriple> traceIdTimestamps = serviceName == null ? traceIdTimeStamps :
        serviceToTraceIdTimeStamp.get(serviceName);
    if (traceIdTimestamps == null || traceIdTimestamps.isEmpty()) return Collections.emptySet();
    Set<Pair<Long>> result = new LinkedHashSet<Pair<Long>>();
    for (LongTriple traceIdTimestamp : traceIdTimestamps) {
      result.add(Pair.create(traceIdTimestamp._1, traceIdTimestamp._2));
    }
    return result;
  }

  static final Comparator<List<Span>> TRACE_DESCENDING = new Comparator<List<Span>>() {
    @Override
    public int compare(List<Span> left, List<Span> right) {
      return right.get(0).compareTo(left.get(0));
    }
  };

  @Override
  public synchronized List<Span> getTrace(long traceId) {
    List<Span> spans = getRawTrace(traceId);
    return spans == null ? null : CorrectForClockSkew.apply(MergeById.apply(spans));
  }

  @Override public List<Span> getTrace(long traceIdHigh, long traceId) {
    List<Span> spans = getRawTrace(traceIdHigh, traceId);
    return spans == null ? null : CorrectForClockSkew.apply(MergeById.apply(spans));
  }

  @Override public synchronized List<Span> getRawTrace(long traceId) {
    List<Span> spans = (List<Span>) traceIdToSpans.get(Pair.create(0L, traceId));
    if (spans == null || spans.isEmpty()) return null;
    return spans;
  }

  @Override public List<Span> getRawTrace(long traceIdHigh, long traceId) {
    List<Span> spans = (List<Span>) traceIdToSpans.get(Pair.create(traceIdHigh, traceId));
    if (spans == null || spans.isEmpty()) return null;
    List<Span> filtered = new ArrayList<Span>(spans);
    Iterator<Span> iterator = filtered.iterator();
    while (iterator.hasNext()) {
      if (iterator.next().traceIdHigh != traceIdHigh) {
        iterator.remove();
      }
    }
    return filtered;
  }

  @Override
  public synchronized List<String> getServiceNames() {
    return sortedList(serviceToTraceIdTimeStamp.keySet());
  }

  @Override
  public synchronized List<String> getSpanNames(String service) {
    if (service == null) return Collections.emptyList();
    service = service.toLowerCase(); // service names are always lowercase!
    return sortedList(serviceToSpanNames.get(service));
  }

  @Override
  public List<DependencyLink> getDependencies(long endTs, @Nullable Long lookback) {
    QueryRequest request = QueryRequest.builder()
        .endTs(endTs)
        .lookback(lookback)
        .limit(Integer.MAX_VALUE).build();

    DependencyLinker linksBuilder = new DependencyLinker();

    for (Collection<Span> trace : getTraces(request)) {
      if (trace.isEmpty()) continue;

      List<DependencyLinkSpan> linkSpans = new LinkedList<DependencyLinkSpan>();
      for (Span s : MergeById.apply(trace)) {
        linkSpans.add(DependencyLinkSpan.from(s));
      }

      linksBuilder.putTrace(linkSpans.iterator());
    }
    return linksBuilder.link();
  }

  static boolean test(QueryRequest request, List<Span> spans) {
    Long timestamp = spans.get(0).timestamp;
    if (timestamp == null ||
        timestamp < (request.endTs - request.lookback) * 1000 ||
        timestamp > request.endTs * 1000) {
      return false;
    }
    Set<String> serviceNames = new LinkedHashSet<String>();
    boolean testedDuration = request.minDuration == null && request.maxDuration == null;

    String spanName = request.spanName;
    Set<String> annotations = new LinkedHashSet<String>(request.annotations);
    Map<String, String> binaryAnnotations = new LinkedHashMap<String, String>(request.binaryAnnotations);

    Set<String> currentServiceNames = new LinkedHashSet<String>();
    for (Span span : spans) {
      currentServiceNames.clear();

      for (Annotation a : span.annotations) {
        if (appliesToServiceName(a.endpoint, request.serviceName)) {
          annotations.remove(a.value);
        }
        if (a.endpoint != null) {
          serviceNames.add(a.endpoint.serviceName);
          currentServiceNames.add(a.endpoint.serviceName);
        }
      }

      for (BinaryAnnotation b : span.binaryAnnotations) {
        if (appliesToServiceName(b.endpoint, request.serviceName) &&
            b.type == BinaryAnnotation.Type.STRING &&
            new String(b.value, UTF_8).equals(binaryAnnotations.get(b.key))) {
          binaryAnnotations.remove(b.key);
        }
        if (b.endpoint != null) {
          serviceNames.add(b.endpoint.serviceName);
          currentServiceNames.add(b.endpoint.serviceName);
        }
      }

      if ((request.serviceName == null || currentServiceNames.contains(request.serviceName))
          && !testedDuration) {
        if (request.minDuration != null && request.maxDuration != null) {
          testedDuration =
              span.duration >= request.minDuration && span.duration <= request.maxDuration;
        } else if (request.minDuration != null) {
          testedDuration = span.duration >= request.minDuration;
        }
      }

      if (span.name.equals(spanName)) {
        spanName = null;
      }
    }
    return (request.serviceName == null || serviceNames.contains(request.serviceName))
        && spanName == null
        && annotations.isEmpty()
        && binaryAnnotations.isEmpty()
        && testedDuration;
  }

  private static boolean appliesToServiceName(Endpoint endpoint, String serviceName) {
    if (serviceName == null) return true;
    if (endpoint == null) return true;
    return endpoint.serviceName.equals(serviceName);
  }

  static final class LinkedListMultimap<K, V> extends Multimap<K, V> {

    @Override Collection<V> valueContainer() {
      return new LinkedList<V>();
    }
  }

  static final Comparator<LongTriple> VALUE_3_DESCENDING = new Comparator<LongTriple>() {
    @Override
    public int compare(LongTriple left, LongTriple right) {
      int result = compare(right._3, left._3);
      if (result != 0) return result;
      result = compare(right._2, left._2);
      if (result != 0) return result;
      return compare(right._1, left._2);
    }

    /** Added to avoid dependency on later versions of Java */
    int compare(long x, long y) {
      return x < y ? -1 : x == y ? 0 : 1;
    }
  };

  /** QueryRequest.limit needs trace ids are returned in timestamp descending order. */
  static final class SortedByValue3Descending<K> extends Multimap<K, LongTriple> {

    @Override Set<LongTriple> valueContainer() {
      return new TreeSet<LongTriple>(VALUE_3_DESCENDING);
    }
  }

  static final class LinkedHashSetMultimap<K, V> extends Multimap<K, V> {

    @Override Collection<V> valueContainer() {
      return new LinkedHashSet<V>();
    }
  }

  static abstract class Multimap<K, V> {
    private final Map<K, Collection<V>> delegate = new LinkedHashMap<K, Collection<V>>();

    abstract Collection<V> valueContainer();

    Set<K> keySet() {
      return delegate.keySet();
    }

    void put(K key, V value) {
      Collection<V> valueContainer = delegate.get(key);
      if (valueContainer == null) {
        synchronized (delegate) {
          if (!delegate.containsKey(key)) {
            valueContainer = valueContainer();
            delegate.put(key, valueContainer);
          }
        }
      }
      valueContainer.add(value);
    }

    // not synchronized as only used for for testing
    void clear() {
      delegate.clear();
    }

    Collection<V> get(K key) {
      return delegate.get(key);
    }
  }

  static final class LongTriple {
    final long _1;
    final long _2;
    final long _3;

    LongTriple(long _1, long _2, long _3) {
      this._1 = _1;
      this._2 = _2;
      this._3 = _3;
    }

    @Override
    public String toString() {
      return "(" + _1 + ", " + _2  + ", " + _3 + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) return true;
      if (o instanceof LongTriple) {
        LongTriple that = (LongTriple) o;
        return this._1 == that._1 && this._2 == that._2 && this._3 == that._3;
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= (_1 >>> 32) ^ _1;
      h *= 1000003;
      h ^= (_2 >>> 32) ^ _2;
      h *= 1000003;
      h ^= (_3 >>> 32) ^ _3;
      return h;
    }
  }
}
