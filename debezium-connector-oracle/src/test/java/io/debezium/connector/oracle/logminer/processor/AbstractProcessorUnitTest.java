/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.processor;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;
import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDefaultValueConverter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.OracleTopicSelector;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Abstract class implementation for all unit tests for {@link LogMinerEventProcessor} implementations.
 *
 * @author Chris Cranford
 */
public abstract class AbstractProcessorUnitTest<T extends AbstractLogMinerEventProcessor> extends AbstractConnectorTest {

    private static final String TRANSACTION_ID_1 = "1234567890";
    private static final String TRANSACTION_ID_2 = "9876543210";

    private static final long PURGED_OBJECT_ID = 162002L;
    private static final String CAPTURE_SET = "DEBEZIUM\\.TEST_TABLE";
    private static final TableId CAPTURED_TABLE = TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE");

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    protected ChangeEventSourceContext context;
    protected EventDispatcher<OraclePartition, TableId> dispatcher;
    protected OracleDatabaseSchema schema;
    protected OracleStreamingChangeEventSourceMetrics metrics;
    protected OraclePartition partition;
    protected OracleOffsetContext offsetContext;
    protected OracleConnection connection;
    protected int outOfBandConnections;

    @Before
    @SuppressWarnings({ "unchecked" })
    public void before() throws Exception {
        this.context = Mockito.mock(ChangeEventSourceContext.class);
        Mockito.when(this.context.isRunning()).thenReturn(true);

        this.dispatcher = (EventDispatcher<OraclePartition, TableId>) Mockito.mock(EventDispatcher.class);
        this.partition = Mockito.mock(OraclePartition.class);
        this.offsetContext = Mockito.mock(OracleOffsetContext.class);
        final CommitScn commitScn = CommitScn.valueOf((String) null);
        Mockito.when(this.offsetContext.getCommitScn()).thenReturn(commitScn);
        this.connection = createOracleConnection();
        this.schema = createOracleDatabaseSchema();
        this.metrics = createMetrics(schema);
    }

    @After
    public void after() {
        if (schema != null) {
            try {
                schema.close();
            }
            finally {
                schema = null;
            }
        }
    }

    protected abstract Configuration.Builder getConfig();

    protected abstract T getProcessor(OracleConnectorConfig connectorConfig);

    protected boolean isTransactionAbandonmentSupported() {
        return true;
    }

    @Test
    public void testCacheIsEmpty() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        try (T processor = getProcessor(config)) {
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenTransactionIsAdded() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName());
        try (T processor = getProcessor(config)) {
            final LogMinerEventRow insertRow = getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1);
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(insertRow);
            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsEmptyWhenTransactionIsRolledBack() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenFirstTransactionIsRolledBack() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_1));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_1)).isTrue();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_2)).isFalse();
        }
    }

    @Test
    public void testCacheIsNotEmptyWhenSecondTransactionIsRolledBack() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            processor.handleRollback(getRollbackLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_2)).isTrue();
            assertThat(metrics.getRolledBackTransactionIds().contains(TRANSACTION_ID_1)).isFalse();
        }
    }

    @Test
    public void testCalculateScnWhenTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(2L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    public void testCalculateScnWhenFirstTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_1));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(3L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(6L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(4L).toString());
        }
    }

    @Test
    public void testCalculateScnWhenSecondTransactionIsCommitted() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        final OraclePartition partition = new OraclePartition(config.getLogicalName());
        try (T processor = getProcessor(config)) {
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));

            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            assertThat(metrics.getOldestScn()).isEqualTo(Scn.valueOf(1L).toString());
            assertThat(metrics.getRolledBackTransactionIds()).isEmpty();
        }
    }

    @Test
    public void testAbandonOneTransaction() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1, changeTime));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
        }
    }

    @Test
    public void testAbandonTransactionHavingAnotherOne() throws Exception {
        if (!isTransactionAbandonmentSupported()) {
            return;
        }

        final OracleConnectorConfig config = new OracleConnectorConfig(getCapturedConfig().build());
        try (T processor = getProcessor(config)) {
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));

            Instant changeTime = Instant.now().minus(24, ChronoUnit.HOURS);
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1, changeTime));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1, changeTime));
            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(4L), TRANSACTION_ID_2));
            processor.handleDataEvent(getInsertLogMinerEventRow(Scn.valueOf(5L), TRANSACTION_ID_2));
            processor.abandonTransactions(Duration.ofHours(1L));
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
            assertThat(processor.getTransactionCache().get(TRANSACTION_ID_1)).isNull();
            assertThat(processor.getTransactionCache().get(TRANSACTION_ID_2)).isNotNull();
        }
    }

    @Test
    public void testRegisteredObjectIdStillResolvesUnderHybridStrategy() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getHybridConfig().build());
        try (T processor = getProcessor(config)) {
            schema.registerTableObjectId(CAPTURED_TABLE, PURGED_OBJECT_ID, null);
            final LogMinerEventRow row = getPurgedObjectLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1);

            processor.handleDataEvent(row);

            Mockito.verify(row).setTableId(CAPTURED_TABLE);
            assertThat(processor.getTransactionCache().isEmpty()).isFalse();
        }
    }

    @Test
    public void testRegistryRejectsTableOutsideTheCaptureSet() {
        final TableId foreign = TableId.parse("ORCLPDB1.SOMEONE_ELSE.FOREIGN_TABLE");

        assertThat(schema.isCapturedTable(foreign)).isFalse();
        assertThat(schema.registerTableObjectId(foreign, PURGED_OBJECT_ID, null)).isFalse();
        assertThat(schema.getTableIdByObjectId(PURGED_OBJECT_ID, null)).isNull();
    }

    @Test
    public void testRegistryAcceptsCapturedTable() {
        final TableId captured = CAPTURED_TABLE;

        assertThat(schema.isCapturedTable(captured)).isTrue();
        assertThat(schema.registerTableObjectId(captured, PURGED_OBJECT_ID, null)).isTrue();
        assertThat(schema.getTableIdByObjectId(PURGED_OBJECT_ID, null)).isEqualTo(captured);
    }

    /** A changed DATA_OBJECT_ID (truncate, move) must not answer from the stale entry. */
    @Test
    public void testDataObjectIdMismatchIsTreatedAsARegistryMiss() {
        assertThat(schema.registerTableObjectId(CAPTURED_TABLE, PURGED_OBJECT_ID, 5L)).isTrue();

        assertThat(schema.getTableIdByObjectId(PURGED_OBJECT_ID, 5L)).isEqualTo(CAPTURED_TABLE);
        assertThat(schema.getTableIdByObjectId(PURGED_OBJECT_ID, 9L)).isNull();
        assertThat(schema.getTableIdByObjectId(PURGED_OBJECT_ID, null)).isEqualTo(CAPTURED_TABLE);
    }

    @Test
    public void testRegisteringTableObjectIdClearsAPriorUnresolvableRecord() {
        assertThat(schema.registerUnresolvableObjectId(PURGED_OBJECT_ID)).isTrue();
        assertThat(schema.registerUnresolvableObjectId(PURGED_OBJECT_ID)).isFalse();

        assertThat(schema.registerTableObjectId(CAPTURED_TABLE, PURGED_OBJECT_ID, null)).isTrue();

        assertThat(schema.isObjectIdUnresolvable(PURGED_OBJECT_ID)).isFalse();
    }

    @Test
    public void testUnresolvableObjectIdCacheIsBounded() throws Exception {
        final int cacheSize = new OracleConnectorConfig(getCapturedConfig().build()).getLogMiningObjectIdCacheSize();
        for (long objectId = 1L; objectId <= cacheSize + 10L; objectId++) {
            schema.registerUnresolvableObjectId(objectId);
        }

        assertThat(schema.isObjectIdUnresolvable(1L)).isFalse();
        assertThat(schema.isObjectIdUnresolvable(cacheSize + 10L)).isTrue();
    }

    /**
     * The production failure: another application's purged table must not stop the connector, a miss
     * must never reach the database, and every discarded event must be warned about and counted.
     */
    @Test
    public void testUnresolvableObjectIdIsSkippedByDefault() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getHybridConfig().build());
        assertThat(config.getUnresolvableObjectIdHandlingMode()).isEqualTo(OracleConnectorConfig.UnresolvableObjectIdHandlingMode.SKIP);
        final LogInterceptor logs = new LogInterceptor(AbstractLogMinerEventProcessor.class);
        try (T processor = getProcessor(config)) {
            final int warningsBefore = metrics.getWarningCount();

            for (int i = 0; i < 3; i++) {
                processor.handleDataEvent(getPurgedObjectLogMinerEventRow(Scn.valueOf(2L + i), TRANSACTION_ID_1));
            }
            processor.processResults(partition, Mockito.mock(ResultSet.class));

            assertThat(processor.getTransactionCache().isEmpty()).isTrue();
            assertThat(metrics.getWarningCount()).isEqualTo(warningsBefore + 1);
            assertThat(logs.containsWarnMessage("Failed to resolve table name by object id " + PURGED_OBJECT_ID)).isTrue();
            assertThat(logs.containsWarnMessage("Skipped 3 change events across 1 unresolvable object ids")).isTrue();
            assertThat(outOfBandConnections).isEqualTo(0);
        }
    }

    @Test(expected = DebeziumException.class)
    public void testInheritDefersToTheFailureHandlingMode() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getHybridConfig()
                .with("internal.log.mining.unresolvable.object.id.handling.mode", "inherit")
                .build());
        try (T processor = getProcessor(config)) {
            processor.handleDataEvent(getPurgedObjectLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
        }
    }

    /** Without the recycle-bin hole a dropped captured table's trailing rows die in Oracle; this is the only signal. */
    @Test
    public void testDroppingACapturedTableWarns() throws Exception {
        Mockito.when(partition.getSourcePartition()).thenReturn(Collections.emptyMap());
        Mockito.when(offsetContext.getOffset()).thenReturn(Collections.emptyMap());
        Mockito.when(offsetContext.getSourceInfo()).thenReturn(Mockito.mock(Struct.class));

        assertThat(dropTableWarns(CAPTURED_TABLE)).isTrue();
        assertThat(dropTableWarns(TableId.parse("ORCLPDB1.DEBEZIUM.OTHER_T"))).isFalse();
    }

    /**
     * End to end: a resolved event must be DISPATCHED under the resolved table, not merely parsed
     * with it. Dispatch filters on the identifier the event carries, so an event emitted under
     * "UNKNOWN.OBJ# <n>" is discarded no matter how correctly it was resolved and parsed.
     */
    @Test
    public void testResolvedEventIsDispatchedUnderTheResolvedTable() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getHybridConfig().build());
        try (T processor = getProcessor(config)) {
            schema.registerTableObjectId(CAPTURED_TABLE, PURGED_OBJECT_ID, null);
            Mockito.when(offsetContext.getScn()).thenReturn(Scn.valueOf(1L));

            processor.handleStart(getStartLogMinerEventRow(Scn.valueOf(1L), TRANSACTION_ID_1));
            processor.handleDataEvent(getPurgedObjectLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1));
            processor.handleCommit(partition, getCommitLogMinerEventRow(Scn.valueOf(3L), TRANSACTION_ID_1));

            Mockito.verify(dispatcher).dispatchDataChangeEvent(Mockito.any(), Mockito.eq(CAPTURED_TABLE), Mockito.any());
        }
    }

    /** A row LogMiner named correctly must not be rewritten. */
    @Test
    public void testNamedRowIdentityIsLeftAlone() throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getHybridConfig().build());
        try (T processor = getProcessor(config)) {
            final LogMinerEventRow row = getInsertLogMinerEventRow(Scn.valueOf(2L), TRANSACTION_ID_1);

            processor.handleDataEvent(row);

            Mockito.verify(row, Mockito.never()).setTableId(Mockito.any());
        }
    }

    /** F5: a rename out of the capture set must not leave the object id pointing at the old table. */
    @Test
    public void testRenameOutOfTheCaptureSetDropsTheMapping() {
        assertThat(schema.registerTableObjectId(CAPTURED_TABLE, PURGED_OBJECT_ID, null)).isTrue();

        // The rename DDL re-registers the same object id under a name outside the capture set.
        assertThat(schema.registerTableObjectId(TableId.parse("ORCLPDB1.DEBEZIUM.OTHER_T"), PURGED_OBJECT_ID, null)).isFalse();

        assertThat(schema.getTableIdByObjectId(PURGED_OBJECT_ID, null)).isNull();
    }

    /** The schema and the processor must agree on the capture set; production never has them differ. */
    private Configuration.Builder getCapturedConfig() {
        return getConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, CAPTURE_SET);
    }

    private Configuration.Builder getHybridConfig() {
        return getCapturedConfig().with(OracleConnectorConfig.LOG_MINING_STRATEGY, "hybrid");
    }

    private boolean dropTableWarns(TableId tableId) throws Exception {
        final OracleConnectorConfig config = new OracleConnectorConfig(getHybridConfig().build());
        final LogInterceptor logs = new LogInterceptor(OracleSchemaChangeEventEmitter.class);
        final String ddl = "DROP TABLE " + tableId.schema() + "." + tableId.table() + ";";
        final OracleSchemaChangeEventEmitter emitter = new OracleSchemaChangeEventEmitter(config, partition, offsetContext,
                tableId, tableId.catalog(), tableId.schema(), ddl, schema, Instant.now(), metrics, () -> {
                });
        emitter.emitSchemaChangeEvent(Mockito.mock(SchemaChangeEventEmitter.Receiver.class));
        return logs.containsWarnMessage("Captured table " + tableId + " was dropped");
    }

    /** An explicit capture set: the default configuration includes every table, so nothing is outside it. */
    private OracleDatabaseSchema createOracleDatabaseSchema() throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(getCapturedConfig().build());
        final TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjustmentMode().createAdjuster();
        final OracleValueConverters converters = new OracleValueConverters(connectorConfig, connection);
        final OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(converters, connection);
        final TableNameCaseSensitivity sensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(connection);

        final OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig,
                converters,
                defaultValueConverter,
                schemaNameAdjuster,
                topicSelector,
                sensitivity);

        Table table = Table.editor()
                .tableId(CAPTURED_TABLE)
                .addColumn(Column.editor().name("ID").create())
                .addColumn(Column.editor().name("DATA").create())
                .create();

        schema.refresh(table);
        return schema;
    }

    private OracleConnection createOracleConnection() throws Exception {
        final ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.next()).thenReturn(true);
        Mockito.when(rs.getFloat(1)).thenReturn(2.f);

        final PreparedStatement stmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(stmt.executeQuery()).thenReturn(rs);

        final Connection conn = Mockito.mock(Connection.class);
        Mockito.when(conn.prepareStatement(Mockito.any())).thenReturn(stmt);

        OracleConnection connection = Mockito.mock(OracleConnection.class);
        Mockito.when(connection.connection(Mockito.anyBoolean())).thenReturn(conn);
        Mockito.when(connection.singleOptionalValue(anyString(), any())).thenReturn(2.f);
        return connection;
    }

    private OracleStreamingChangeEventSourceMetrics createMetrics(OracleDatabaseSchema schema) throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(getCapturedConfig().build());
        final OracleTaskContext taskContext = new OracleTaskContext(connectorConfig, schema);

        final ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(Duration.of(DEFAULT_MAX_QUEUE_SIZE, ChronoUnit.MILLIS))
                .maxBatchSize(DEFAULT_MAX_BATCH_SIZE)
                .maxQueueSize(DEFAULT_MAX_QUEUE_SIZE)
                .build();

        return new OracleStreamingChangeEventSourceMetrics(taskContext, queue, null, connectorConfig);
    }

    private LogMinerEventRow getStartLogMinerEventRow(Scn scn, String transactionId) {
        return getStartLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getStartLogMinerEventRow(Scn scn, String transactionId, Instant changeTime) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.START);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        return row;
    }

    private LogMinerEventRow getCommitLogMinerEventRow(Scn scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.COMMIT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        return row;
    }

    private LogMinerEventRow getRollbackLogMinerEventRow(Scn scn, String transactionId) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.ROLLBACK);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(Instant.now());
        return row;
    }

    private LogMinerEventRow getInsertLogMinerEventRow(Scn scn, String transactionId) {
        return getInsertLogMinerEventRow(scn, transactionId, Instant.now());
    }

    private LogMinerEventRow getInsertLogMinerEventRow(Scn scn, String transactionId, Instant changeTime) {
        LogMinerEventRow row = Mockito.mock(LogMinerEventRow.class);
        Mockito.when(row.getEventType()).thenReturn(EventType.INSERT);
        Mockito.when(row.getTransactionId()).thenReturn(transactionId);
        Mockito.when(row.getScn()).thenReturn(scn);
        Mockito.when(row.getChangeTime()).thenReturn(changeTime);
        Mockito.when(row.getRowId()).thenReturn("1234567890");
        Mockito.when(row.getOperation()).thenReturn("INSERT");
        Mockito.when(row.getTableName()).thenReturn("TEST_TABLE");
        Mockito.when(row.getTableId()).thenReturn(CAPTURED_TABLE);
        Mockito.when(row.getRedoSql()).thenReturn("insert into \"DEBEZIUM\".\"TEST_TABLE\"(\"ID\",\"DATA\") values ('1','Test');");
        Mockito.when(row.getRsId()).thenReturn("A.B.C");
        Mockito.when(row.getTablespaceName()).thenReturn("DEBEZIUM");
        Mockito.when(row.getUserName()).thenReturn(TestHelper.SCHEMA_USER);
        return row;
    }

    private LogMinerEventRow getPurgedObjectLogMinerEventRow(Scn scn, String transactionId) {
        LogMinerEventRow row = getInsertLogMinerEventRow(scn, transactionId, Instant.now());
        Mockito.when(row.getTableName()).thenReturn("OBJ# " + PURGED_OBJECT_ID);
        Mockito.when(row.getTablespaceName()).thenReturn("UNKNOWN");
        Mockito.when(row.getObjectId()).thenReturn(PURGED_OBJECT_ID);
        // The real row is mutable and the event is dispatched under whatever it ends up holding, so a
        // mock that ignored setTableId would hide exactly the defect these tests exist to catch.
        final AtomicReference<TableId> identity = new AtomicReference<>(new TableId("ORCLPDB1", "UNKNOWN", "OBJ# " + PURGED_OBJECT_ID));
        Mockito.when(row.getTableId()).thenAnswer(invocation -> identity.get());
        Mockito.doAnswer(invocation -> {
            identity.set(invocation.getArgument(0));
            return null;
        }).when(row).setTableId(Mockito.any());
        return row;
    }

}
