/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adbc.driver.flightsql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.sql.SqlQuirks;
import org.apache.arrow.driver.jdbc.utils.MockFlightSqlProducer;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies how {@link FlightSqlConnection#close()} interacts with the Flight SQL {@code
 * CloseSession} RPC: it is only sent when the {@code close_on_disconnect} option is enabled, and a
 * failure of that RPC is surfaced to the caller without leaking local resources.
 */
public class CloseSessionTest {

  private RootAllocator allocator;
  private SessionTrackingProducer producer;
  private FlightServer server;
  private Location location;

  @BeforeEach
  public void setUp() throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    producer = new SessionTrackingProducer();
    server =
        FlightServer.builder()
            .allocator(allocator)
            .location(Location.forGrpcInsecure("localhost", 0))
            .producer(producer)
            .build();
    server.start();
    location = Location.forGrpcInsecure("localhost", server.getPort());
  }

  @AfterEach
  public void tearDown() throws Exception {
    AutoCloseables.close(server, allocator);
  }

  private Map<String, Object> params(boolean closeSessionOnClose) {
    final Map<String, Object> params = new HashMap<>();
    if (closeSessionOnClose) {
      params.put(FlightSqlConnectionProperties.CLOSE_SESSION_ON_CLOSE.getKey(), true);
    }
    return params;
  }

  /**
   * Builds a connection on a dedicated child allocator so the test can assert that {@code close()}
   * releases it.
   */
  private FlightSqlConnection connect(boolean closeSessionOnClose) throws Exception {
    final BufferAllocator connectionAllocator =
        allocator.newChildAllocator("adbc-flight-connection-test", 0, allocator.getLimit());
    try {
      return new FlightSqlConnection(
          connectionAllocator, new SqlQuirks(), location, params(closeSessionOnClose));
    } catch (Exception e) {
      AutoCloseables.close(connectionAllocator);
      throw e;
    }
  }

  private boolean hasConnectionChildAllocator() {
    return allocator.getChildAllocators().stream()
        .anyMatch(child -> child.getName().startsWith("adbc-flight-connection-test"));
  }

  @Test
  public void closeSessionSentWhenEnabled() throws Exception {
    producer.closeSessionStatus = CloseSessionResult.Status.CLOSED;
    final FlightSqlConnection connection = connect(true);

    connection.close();

    assertEquals(1, producer.closeSessionCalls.get(), "CloseSession should be sent exactly once");
    assertFalse(hasConnectionChildAllocator(), "connection allocator should be released");
  }

  @Test
  public void closeSessionNotSentWhenDisabled() throws Exception {
    final FlightSqlConnection connection = connect(false);

    connection.close();

    assertEquals(0, producer.closeSessionCalls.get(), "CloseSession should not be sent");
    assertFalse(hasConnectionChildAllocator(), "connection allocator should be released");
  }

  @Test
  public void closeSessionFailureIsSurfaced() throws Exception {
    producer.failCloseSession = true;
    final FlightSqlConnection connection = connect(true);

    final AdbcException thrown = assertThrows(AdbcException.class, connection::close);

    assertEquals(1, producer.closeSessionCalls.get(), "CloseSession should be attempted once");
    assertTrue(
        thrown.getMessage().contains("Failed to close session"),
        "exception should describe the session-close failure: " + thrown.getMessage());
  }

  @Test
  public void closeSessionFailureStillReleasesResources() throws Exception {
    producer.failCloseSession = true;
    final FlightSqlConnection connection = connect(true);

    assertThrows(AdbcException.class, connection::close);

    assertFalse(
        hasConnectionChildAllocator(),
        "connection allocator must be released even when CloseSession fails");
  }

  /**
   * A Flight SQL producer that records and controls the {@code CloseSession} RPC. {@link
   * MockFlightSqlProducer} is final, so this delegates every other call to an embedded instance and
   * only overrides {@code closeSession}.
   */
  private static final class SessionTrackingProducer implements FlightSqlProducer {
    private final MockFlightSqlProducer delegate = new MockFlightSqlProducer();

    final AtomicInteger closeSessionCalls = new AtomicInteger();
    volatile boolean failCloseSession = false;
    volatile CloseSessionResult.Status closeSessionStatus = CloseSessionResult.Status.CLOSED;

    @Override
    public void closeSession(
        CloseSessionRequest request,
        CallContext context,
        StreamListener<CloseSessionResult> listener) {
      closeSessionCalls.incrementAndGet();
      if (failCloseSession) {
        listener.onError(
            CallStatus.INTERNAL.withDescription("simulated failure").toRuntimeException());
        return;
      }
      listener.onNext(new CloseSessionResult(closeSessionStatus));
      listener.onCompleted();
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }

    @Override
    public void listFlights(
        CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
      delegate.listFlights(context, criteria, listener);
    }

    @Override
    public void createPreparedStatement(
        FlightSql.ActionCreatePreparedStatementRequest request,
        CallContext context,
        StreamListener<Result> listener) {
      delegate.createPreparedStatement(request, context, listener);
    }

    @Override
    public void closePreparedStatement(
        FlightSql.ActionClosePreparedStatementRequest request,
        CallContext context,
        StreamListener<Result> listener) {
      delegate.closePreparedStatement(request, context, listener);
    }

    @Override
    public FlightInfo getFlightInfoStatement(
        FlightSql.CommandStatementQuery command,
        CallContext context,
        FlightDescriptor descriptor) {
      return delegate.getFlightInfoStatement(command, context, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
        FlightSql.CommandPreparedStatementQuery command,
        CallContext context,
        FlightDescriptor descriptor) {
      return delegate.getFlightInfoPreparedStatement(command, context, descriptor);
    }

    @Override
    public SchemaResult getSchemaStatement(
        FlightSql.CommandStatementQuery command,
        CallContext context,
        FlightDescriptor descriptor) {
      return delegate.getSchemaStatement(command, context, descriptor);
    }

    @Override
    public void getStreamStatement(
        FlightSql.TicketStatementQuery ticket,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamStatement(ticket, context, listener);
    }

    @Override
    public void getStreamPreparedStatement(
        FlightSql.CommandPreparedStatementQuery command,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamPreparedStatement(command, context, listener);
    }

    @Override
    public Runnable acceptPutStatement(
        FlightSql.CommandStatementUpdate command,
        CallContext context,
        FlightStream stream,
        StreamListener<PutResult> listener) {
      return delegate.acceptPutStatement(command, context, stream, listener);
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(
        FlightSql.CommandPreparedStatementUpdate command,
        CallContext context,
        FlightStream stream,
        StreamListener<PutResult> listener) {
      return delegate.acceptPutPreparedStatementUpdate(command, context, stream, listener);
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(
        FlightSql.CommandPreparedStatementQuery command,
        CallContext context,
        FlightStream stream,
        StreamListener<PutResult> listener) {
      return delegate.acceptPutPreparedStatementQuery(command, context, stream, listener);
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(
        FlightSql.CommandGetSqlInfo command, CallContext context, FlightDescriptor descriptor) {
      return delegate.getFlightInfoSqlInfo(command, context, descriptor);
    }

    @Override
    public void getStreamSqlInfo(
        FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
      delegate.getStreamSqlInfo(command, context, listener);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(
        FlightSql.CommandGetXdbcTypeInfo command,
        CallContext context,
        FlightDescriptor descriptor) {
      return delegate.getFlightInfoTypeInfo(command, context, descriptor);
    }

    @Override
    public void getStreamTypeInfo(
        FlightSql.CommandGetXdbcTypeInfo command,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamTypeInfo(command, context, listener);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(
        FlightSql.CommandGetCatalogs command, CallContext context, FlightDescriptor descriptor) {
      return delegate.getFlightInfoCatalogs(command, context, descriptor);
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
      delegate.getStreamCatalogs(context, listener);
    }

    @Override
    public FlightInfo getFlightInfoSchemas(
        FlightSql.CommandGetDbSchemas command, CallContext context, FlightDescriptor descriptor) {
      return delegate.getFlightInfoSchemas(command, context, descriptor);
    }

    @Override
    public void getStreamSchemas(
        FlightSql.CommandGetDbSchemas command,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamSchemas(command, context, listener);
    }

    @Override
    public FlightInfo getFlightInfoTables(
        FlightSql.CommandGetTables command, CallContext context, FlightDescriptor descriptor) {
      return delegate.getFlightInfoTables(command, context, descriptor);
    }

    @Override
    public void getStreamTables(
        FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
      delegate.getStreamTables(command, context, listener);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(
        FlightSql.CommandGetTableTypes command, CallContext context, FlightDescriptor descriptor) {
      return delegate.getFlightInfoTableTypes(command, context, descriptor);
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
      delegate.getStreamTableTypes(context, listener);
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(
        FlightSql.CommandGetPrimaryKeys command, CallContext context, FlightDescriptor descriptor) {
      return delegate.getFlightInfoPrimaryKeys(command, context, descriptor);
    }

    @Override
    public void getStreamPrimaryKeys(
        FlightSql.CommandGetPrimaryKeys command,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamPrimaryKeys(command, context, listener);
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(
        FlightSql.CommandGetExportedKeys command,
        CallContext context,
        FlightDescriptor descriptor) {
      return delegate.getFlightInfoExportedKeys(command, context, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(
        FlightSql.CommandGetImportedKeys command,
        CallContext context,
        FlightDescriptor descriptor) {
      return delegate.getFlightInfoImportedKeys(command, context, descriptor);
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(
        FlightSql.CommandGetCrossReference command,
        CallContext context,
        FlightDescriptor descriptor) {
      return delegate.getFlightInfoCrossReference(command, context, descriptor);
    }

    @Override
    public void getStreamExportedKeys(
        FlightSql.CommandGetExportedKeys command,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamExportedKeys(command, context, listener);
    }

    @Override
    public void getStreamImportedKeys(
        FlightSql.CommandGetImportedKeys command,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamImportedKeys(command, context, listener);
    }

    @Override
    public void getStreamCrossReference(
        FlightSql.CommandGetCrossReference command,
        CallContext context,
        ServerStreamListener listener) {
      delegate.getStreamCrossReference(command, context, listener);
    }
  }
}
