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
package zipkin.storage.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.jooq.Condition;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.Row2;
import org.jooq.Row3;
import org.jooq.SelectConditionStep;
import org.jooq.SelectField;
import org.jooq.SelectOffsetStep;
import org.jooq.TableField;
import org.jooq.TableOnConditionStep;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.BinaryAnnotation.Type;
import zipkin.DependencyLink;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.internal.ApplyTimestampAndDuration;
import zipkin.internal.CorrectForClockSkew;
import zipkin.internal.DependencyLinkSpan;
import zipkin.internal.DependencyLinker;
import zipkin.internal.Lazy;
import zipkin.internal.Nullable;
import zipkin.storage.QueryRequest;
import zipkin.storage.SpanStore;
import zipkin.storage.mysql.internal.generated.tables.ZipkinAnnotations;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static org.jooq.impl.DSL.row;
import static zipkin.BinaryAnnotation.Type.STRING;
import static zipkin.Constants.CLIENT_ADDR;
import static zipkin.Constants.CLIENT_SEND;
import static zipkin.Constants.SERVER_ADDR;
import static zipkin.Constants.SERVER_RECV;
import static zipkin.internal.Util.UTF_8;
import static zipkin.internal.Util.getDays;
import static zipkin.storage.mysql.internal.generated.tables.ZipkinAnnotations.ZIPKIN_ANNOTATIONS;
import static zipkin.storage.mysql.internal.generated.tables.ZipkinDependencies.ZIPKIN_DEPENDENCIES;
import static zipkin.storage.mysql.internal.generated.tables.ZipkinSpans.ZIPKIN_SPANS;

final class MySQLSpanStore implements SpanStore {
  static final Field<?>[] SPAN_FIELDS_WITHOUT_TRACE_ID_HIGH =
      fieldsExcept(ZIPKIN_SPANS.fields(), ZIPKIN_SPANS.TRACE_ID_HIGH);
  static final Field<?>[] ANNOTATION_FIELDS_WITHOUT_TRACE_ID_HIGH =
      fieldsExcept(ZIPKIN_ANNOTATIONS.fields(), ZIPKIN_ANNOTATIONS.TRACE_ID_HIGH);
  static final Field<?>[] ANNOTATION_FIELDS_WITHOUT_IPV6 =
      fieldsExcept(ANNOTATION_FIELDS_WITHOUT_TRACE_ID_HIGH, ZIPKIN_ANNOTATIONS.ENDPOINT_IPV6);
  static final Field<?>[] LINK_FIELDS = new Field<?>[] {
      ZIPKIN_SPANS.TRACE_ID_HIGH, ZIPKIN_SPANS.TRACE_ID, ZIPKIN_SPANS.PARENT_ID, ZIPKIN_SPANS.ID,
      ZIPKIN_ANNOTATIONS.A_KEY, ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME
  };
  static final Field<?>[] LINK_FIELDS_WITHOUT_TRACE_ID_HIGH =
      fieldsExcept(LINK_FIELDS, ZIPKIN_SPANS.TRACE_ID_HIGH);
  static final Row2<Long, Long> SPAN_TRACE_ID_FIELDS =
      row(ZIPKIN_SPANS.TRACE_ID_HIGH, ZIPKIN_SPANS.TRACE_ID);
  static final Row2<Long, Long> ANNOTATION_TRACE_ID_FIELDS =
      row(ZIPKIN_ANNOTATIONS.TRACE_ID_HIGH, ZIPKIN_ANNOTATIONS.TRACE_ID);

  private final DataSource datasource;
  private final DSLContexts context;
  private final Lazy<Boolean> hasTraceIdHigh;
  private final Lazy<Boolean> hasIpv6;
  private final Lazy<Boolean> hasPreAggregatedDependencies;

  MySQLSpanStore(DataSource datasource, DSLContexts context, Lazy<Boolean> hasTraceIdHigh,
      Lazy<Boolean> hasIpv6, Lazy<Boolean> hasPreAggregatedDependencies) {
    this.datasource = datasource;
    this.context = context;
    this.hasTraceIdHigh = hasTraceIdHigh;
    this.hasIpv6 = hasIpv6;
    this.hasPreAggregatedDependencies = hasPreAggregatedDependencies;
  }

  private Endpoint endpoint(Record a) {
    String serviceName = a.getValue(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME);
    if (serviceName == null) return null;
    return Endpoint.builder()
        .serviceName(serviceName)
        .port(a.getValue(ZIPKIN_ANNOTATIONS.ENDPOINT_PORT))
        .ipv4(a.getValue(ZIPKIN_ANNOTATIONS.ENDPOINT_IPV4))
        .ipv6(hasIpv6.get() ? a.getValue(ZIPKIN_ANNOTATIONS.ENDPOINT_IPV6) : null).build();
  }

  SelectOffsetStep<? extends Record> toTraceIdQuery(DSLContext context, QueryRequest request) {
    long endTs = (request.endTs > 0 && request.endTs != Long.MAX_VALUE) ? request.endTs * 1000
        : System.currentTimeMillis() * 1000;

    Condition identifiers = (hasTraceIdHigh.get() ?
        SPAN_TRACE_ID_FIELDS.eq(ANNOTATION_TRACE_ID_FIELDS)
        : ZIPKIN_SPANS.TRACE_ID.eq(ZIPKIN_ANNOTATIONS.TRACE_ID))
        .and(ZIPKIN_SPANS.ID.eq(ZIPKIN_ANNOTATIONS.SPAN_ID));

    TableOnConditionStep<?> table = ZIPKIN_SPANS.join(ZIPKIN_ANNOTATIONS).on(identifiers);

    int i = 0;
    for (String key : request.annotations) {
      ZipkinAnnotations aTable = ZIPKIN_ANNOTATIONS.as("a" + i++);
      Condition aIdentifiers = (hasTraceIdHigh.get() ?
          SPAN_TRACE_ID_FIELDS.eq(row(aTable.TRACE_ID_HIGH, aTable.TRACE_ID))
          : ZIPKIN_SPANS.TRACE_ID.eq(aTable.TRACE_ID))
          .and(ZIPKIN_SPANS.ID.eq(aTable.SPAN_ID));
      table = maybeOnService(table.join(aTable)
          .on(aIdentifiers)
          .and(aTable.A_TYPE.eq(-1))
          .and(aTable.A_KEY.eq(key)), aTable, request.serviceName);
    }

    for (Map.Entry<String, String> kv : request.binaryAnnotations.entrySet()) {
      ZipkinAnnotations aTable = ZIPKIN_ANNOTATIONS.as("a" + i++);
      Condition aIdentifiers = (hasTraceIdHigh.get() ?
          SPAN_TRACE_ID_FIELDS.eq(row(aTable.TRACE_ID_HIGH, aTable.TRACE_ID))
          : ZIPKIN_SPANS.TRACE_ID.eq(aTable.TRACE_ID))
          .and(ZIPKIN_SPANS.ID.eq(aTable.SPAN_ID));
      table = maybeOnService(table.join(aTable)
          .on(aIdentifiers)
          .and(aTable.A_TYPE.eq(STRING.value))
          .and(aTable.A_KEY.eq(kv.getKey()))
          .and(aTable.A_VALUE.eq(kv.getValue().getBytes(UTF_8))), aTable, request.serviceName);
    }
    List<SelectField<?>> traceIdFields = hasTraceIdHigh.get()
        ? asList(ZIPKIN_SPANS.TRACE_ID_HIGH, ZIPKIN_SPANS.TRACE_ID)
        : asList(ZIPKIN_SPANS.TRACE_ID);

    SelectConditionStep<Record> dsl = context.selectDistinct(traceIdFields)
        .from(table)
        .where(ZIPKIN_SPANS.START_TS.between(endTs - request.lookback * 1000, endTs));

    if (request.serviceName != null) {
      dsl.and(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME.eq(request.serviceName));
    }

    if (request.spanName != null) {
      dsl.and(ZIPKIN_SPANS.NAME.eq(request.spanName));
    }

    if (request.minDuration != null && request.maxDuration != null) {
      dsl.and(ZIPKIN_SPANS.DURATION.between(request.minDuration, request.maxDuration));
    } else if (request.minDuration != null) {
      dsl.and(ZIPKIN_SPANS.DURATION.greaterOrEqual(request.minDuration));
    }
    return dsl.orderBy(ZIPKIN_SPANS.START_TS.desc()).limit(request.limit);
  }

  static TableOnConditionStep<?> maybeOnService(TableOnConditionStep<Record> table,
      ZipkinAnnotations aTable, String serviceName) {
    if (serviceName == null) return table;
    return table.and(aTable.ENDPOINT_SERVICE_NAME.eq(serviceName));
  }

  List<List<Span>> getTraces(@Nullable QueryRequest request, @Nullable Long traceIdHigh,
      @Nullable Long traceId, boolean raw) {
    final Map<Row2<Long, Long>, List<Span>> spansWithoutAnnotations;
    final Map<Row3<Long, Long, Long>, List<Record>> dbAnnotations;
    boolean shouldGroupBy128Bit = hasTraceIdHigh.get() &&
        (traceIdHigh != null || (request != null && request.groupByTraceIdHigh));
    try (Connection conn = datasource.getConnection()) {
      final Condition traceIdCondition;
      if (request != null) {
        if (hasTraceIdHigh.get()) {
          traceIdCondition = SPAN_TRACE_ID_FIELDS
              .in((Result<? extends Record2<Long, Long>>) toTraceIdQuery(context.get(conn), request)
                  .fetch());
        } else {
          List<Long> traceIds =
              toTraceIdQuery(context.get(conn), request).fetch(ZIPKIN_SPANS.TRACE_ID);
          traceIdCondition = ZIPKIN_SPANS.TRACE_ID.in(traceIds);
        }
      } else if (traceIdHigh != null && hasTraceIdHigh.get()) {
        traceIdCondition = SPAN_TRACE_ID_FIELDS.eq(traceIdHigh, traceId);
      } else {
        traceIdCondition = ZIPKIN_SPANS.TRACE_ID.eq(traceId);
      }
      spansWithoutAnnotations = context.get(conn)
          .select(hasTraceIdHigh.get() ? ZIPKIN_SPANS.fields() : SPAN_FIELDS_WITHOUT_TRACE_ID_HIGH)
          .from(ZIPKIN_SPANS).where(traceIdCondition)
          .stream()
          .map(r -> Span.builder()
              .traceIdHigh(hasTraceIdHigh.get() ? r.getValue(ZIPKIN_SPANS.TRACE_ID_HIGH) : 0)
              .traceId(r.getValue(ZIPKIN_SPANS.TRACE_ID))
              .name(r.getValue(ZIPKIN_SPANS.NAME))
              .id(r.getValue(ZIPKIN_SPANS.ID))
              .parentId(r.getValue(ZIPKIN_SPANS.PARENT_ID))
              .timestamp(r.getValue(ZIPKIN_SPANS.START_TS))
              .duration(r.getValue(ZIPKIN_SPANS.DURATION))
              .debug(r.getValue(ZIPKIN_SPANS.DEBUG))
              .build())
          .collect(
              groupingBy((Span s) -> row(shouldGroupBy128Bit ? s.traceIdHigh: 0L, s.traceId), LinkedHashMap::new, Collectors.<Span>toList()));

      dbAnnotations = context.get(conn)
          .select(hasTraceIdHigh.get() ? ZIPKIN_ANNOTATIONS.fields()
              : hasIpv6.get() ? ANNOTATION_FIELDS_WITHOUT_TRACE_ID_HIGH
                  : ANNOTATION_FIELDS_WITHOUT_IPV6)
          .from(ZIPKIN_ANNOTATIONS)
          .where(shouldGroupBy128Bit
              ? ANNOTATION_TRACE_ID_FIELDS.in(spansWithoutAnnotations.keySet())
              : ZIPKIN_ANNOTATIONS.TRACE_ID.in(toTraceIds(spansWithoutAnnotations.keySet())))
          .orderBy(ZIPKIN_ANNOTATIONS.A_TIMESTAMP.asc(), ZIPKIN_ANNOTATIONS.A_KEY.asc())
          .stream()
          .collect(groupingBy((Record a) -> row(
              shouldGroupBy128Bit ? a.getValue(ZIPKIN_ANNOTATIONS.TRACE_ID_HIGH) : 0L,
              a.getValue(ZIPKIN_ANNOTATIONS.TRACE_ID),
              a.getValue(ZIPKIN_ANNOTATIONS.SPAN_ID)
              ), LinkedHashMap::new,
              Collectors.<Record>toList())); // LinkedHashMap preserves order while grouping
    } catch (SQLException e) {
      throw new RuntimeException("Error querying for " + request + ": " + e.getMessage());
    }

    List<List<Span>> result = new ArrayList<>(spansWithoutAnnotations.keySet().size());
    for (List<Span> spans : spansWithoutAnnotations.values()) {
      List<Span> trace = new ArrayList<>(spans.size());
      for (Span s : spans) {
        Span.Builder span = s.toBuilder();
        Row3<Long, Long, Long> key = row(shouldGroupBy128Bit ? s.traceIdHigh : 0L, s.traceId, s.id);

        if (dbAnnotations.containsKey(key)) {
          for (Record a : dbAnnotations.get(key)) {
            Endpoint endpoint = endpoint(a);
            int type = a.getValue(ZIPKIN_ANNOTATIONS.A_TYPE);
            if (type == -1) {
              span.addAnnotation(Annotation.create(
                  a.getValue(ZIPKIN_ANNOTATIONS.A_TIMESTAMP),
                  a.getValue(ZIPKIN_ANNOTATIONS.A_KEY),
                  endpoint));
            } else {
              span.addBinaryAnnotation(BinaryAnnotation.create(
                  a.getValue(ZIPKIN_ANNOTATIONS.A_KEY),
                  a.getValue(ZIPKIN_ANNOTATIONS.A_VALUE),
                  Type.fromValue(type),
                  endpoint));
            }
          }
        }
        Span rawSpan = span.build();
        trace.add(raw ? rawSpan : ApplyTimestampAndDuration.apply(rawSpan));
      }
      if (!raw) trace = CorrectForClockSkew.apply(trace);
      result.add(trace);
    }
    if (!raw) Collections.sort(result, (left, right) -> right.get(0).compareTo(left.get(0)));
    return result;
  }

  static Field[] toTraceIds(Collection<Row2<Long, Long>> traceId128s) {
    Field[] result = new Field[traceId128s.size()];
    int i = 0;
    for (Row2<Long, Long> traceId128 : traceId128s) {
      result[i++] = traceId128.field2();
    }
    return result;
  }

  @Override
  public List<List<Span>> getTraces(QueryRequest request) {
    return getTraces(request, null, null, false);
  }

  @Override
  public List<Span> getTrace(long traceId) {
    List<List<Span>> result = getTraces(null, null, traceId, false);
    return result.isEmpty() ? null : result.get(0);
  }

  @Override public List<Span> getTrace(long traceIdHigh, long traceId) {
    List<List<Span>> result = getTraces(null, traceIdHigh, traceId, false);
    return result.isEmpty() ? null : result.get(0);
  }

  @Override
  public List<Span> getRawTrace(long traceId) {
    List<List<Span>> result = getTraces(null, null, traceId, true);
    return result.isEmpty() ? null : result.get(0);
  }

  @Override public List<Span> getRawTrace(long traceIdHigh, long traceId) {
    List<List<Span>> result = getTraces(null, traceIdHigh, traceId, true);
    return result.isEmpty() ? null : result.get(0);
  }

  @Override
  public List<String> getServiceNames() {
    try (Connection conn = datasource.getConnection()) {
      return context.get(conn)
          .selectDistinct(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME)
          .from(ZIPKIN_ANNOTATIONS)
          .where(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME.isNotNull()
              .and(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME.ne("")))
          .fetch(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME);
    } catch (SQLException e) {
      throw new RuntimeException("Error querying for " + e + ": " + e.getMessage());
    }
  }

  @Override
  public List<String> getSpanNames(String serviceName) {
    if (serviceName == null) return emptyList();
    serviceName = serviceName.toLowerCase(); // service names are always lowercase!
    try (Connection conn = datasource.getConnection()) {
      return context.get(conn)
          .selectDistinct(ZIPKIN_SPANS.NAME)
          .from(ZIPKIN_SPANS)
          .join(ZIPKIN_ANNOTATIONS)
          .on(ZIPKIN_SPANS.TRACE_ID.eq(ZIPKIN_ANNOTATIONS.TRACE_ID))
          .and(ZIPKIN_SPANS.ID.eq(ZIPKIN_ANNOTATIONS.SPAN_ID))
          .where(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME.eq(serviceName))
          .orderBy(ZIPKIN_SPANS.NAME)
          .fetch(ZIPKIN_SPANS.NAME);
    } catch (SQLException e) {
      throw new RuntimeException("Error querying for " + serviceName + ": " + e.getMessage());
    }
  }

  @Override
  public List<DependencyLink> getDependencies(long endTs, @Nullable Long lookback) {
    try (Connection conn = datasource.getConnection()) {
      if (hasPreAggregatedDependencies.get()) {
        List<Date> days = getDays(endTs, lookback);
        List<DependencyLink> unmerged = context.get(conn)
            .selectFrom(ZIPKIN_DEPENDENCIES)
            .where(ZIPKIN_DEPENDENCIES.DAY.in(days))
            .fetch((Record l) -> DependencyLink.create(
                l.get(ZIPKIN_DEPENDENCIES.PARENT),
                l.get(ZIPKIN_DEPENDENCIES.CHILD),
                l.get(ZIPKIN_DEPENDENCIES.CALL_COUNT))
            );
        return DependencyLinker.merge(unmerged);
      } else {
        return aggregateDependencies(endTs, lookback, conn);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Error querying dependencies for endTs " + endTs + " and lookback " + lookback + ": " + e.getMessage());
    }
  }

  List<DependencyLink> aggregateDependencies(long endTs, @Nullable Long lookback, Connection conn) {
    endTs = endTs * 1000;
    // Lazy fetching the cursor prevents us from buffering the whole dataset in memory.
    Cursor<Record> cursor = context.get(conn)
        .selectDistinct(hasTraceIdHigh.get() ? LINK_FIELDS : LINK_FIELDS_WITHOUT_TRACE_ID_HIGH)
        // left joining allows us to keep a mapping of all span ids, not just ones that have
        // special annotations. We need all span ids to reconstruct the trace tree. We need
        // the whole trace tree so that we can accurately skip local spans.
        .from(ZIPKIN_SPANS.leftJoin(ZIPKIN_ANNOTATIONS)
            // NOTE: we are intentionally grouping only on the low-bits of trace id. This buys time
            // for applications to upgrade to 128-bit instrumentation.
            .on(ZIPKIN_SPANS.TRACE_ID.eq(ZIPKIN_ANNOTATIONS.TRACE_ID).and(
                ZIPKIN_SPANS.ID.eq(ZIPKIN_ANNOTATIONS.SPAN_ID)))
            .and(ZIPKIN_ANNOTATIONS.A_KEY.in(CLIENT_SEND, CLIENT_ADDR, SERVER_RECV, SERVER_ADDR)))
        .where(lookback == null ?
            ZIPKIN_SPANS.START_TS.lessOrEqual(endTs) :
            ZIPKIN_SPANS.START_TS.between(endTs - lookback * 1000, endTs))
        // Grouping so that later code knows when a span or trace is finished.
        .groupBy(ZIPKIN_SPANS.TRACE_ID, ZIPKIN_SPANS.ID, ZIPKIN_ANNOTATIONS.A_KEY).fetchLazy();

    Iterator<Iterator<DependencyLinkSpan>> traces =
        new DependencyLinkSpanIterator.ByTraceId(cursor.iterator(), hasTraceIdHigh.get());

    if (!traces.hasNext()) return Collections.emptyList();

    DependencyLinker linker = new DependencyLinker();

    while (traces.hasNext()) {
      linker.putTrace(traces.next());
    }

    return linker.link();
  }

  static Field<?>[] fieldsExcept(Field<?>[] fields, TableField<Record, ?> exclude) {
    ArrayList<Field<?>> list = new ArrayList(asList(fields));
    list.remove(exclude);
    list.trimToSize();
    return list.toArray(new Field<?>[0]);
  }
}
