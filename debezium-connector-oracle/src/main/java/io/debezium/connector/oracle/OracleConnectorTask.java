/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Strings;

public class OracleConnectorTask extends BaseSourceTask<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    /**
     * Server-side codes meaning this session is gone. Driver-side failures (17410, 17008, 17002)
     * are absent on purpose: ojdbc raises them as SQLRecoverableException, matched by type below --
     * measured on ojdbc11 21.15, not assumed.
     */
    @Immutable
    private static final Set<Integer> CONNECTION_LOST_ERROR_CODES = Collect.unmodifiableSet(
            28, // your session has been killed
            1012, // not logged on
            2396, // exceeded maximum idle time, please connect again (DBZ-9280)
            2399, // exceeded maximum connect time
            3135); // connection lost contact

    private volatile OracleTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OracleConnection jdbcConnection;
    private volatile OracleConnection heartbeatConnection;
    private volatile ErrorHandler errorHandler;
    private volatile OracleDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> start(Configuration config) {
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjustmentMode().createAdjuster();

        JdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
        jdbcConnection = new OracleConnection(jdbcConfig, () -> getClass().getClassLoader());

        validateRedoLogConfiguration();

        OracleValueConverters valueConverters = new OracleValueConverters(connectorConfig, jdbcConnection);
        OracleDefaultValueConverter defaultValueConverter = new OracleDefaultValueConverter(valueConverters, jdbcConnection);
        TableNameCaseSensitivity tableNameCaseSensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection);
        this.schema = new OracleDatabaseSchema(connectorConfig, valueConverters, defaultValueConverter, schemaNameAdjuster,
                topicSelector, tableNameCaseSensitivity);

        Offsets<OraclePartition, OracleOffsetContext> previousOffsets = getPreviousOffsets(new OraclePartition.Provider(connectorConfig),
                connectorConfig.getAdapter().getOffsetContextLoader());

        OraclePartition partition = previousOffsets.getTheOnlyPartition();
        OracleOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateAndLoadDatabaseHistory(connectorConfig, partition, previousOffset, schema);

        taskContext = new OracleTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new OracleErrorHandler(connectorConfig, queue);

        final OracleEventMetadataProvider metadataProvider = new OracleEventMetadataProvider();

        EventDispatcher<OraclePartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                new HeartbeatFactory<>(
                        connectorConfig,
                        topicSelector,
                        schemaNameAdjuster,
                        () -> getHeartbeatConnection(connectorConfig, jdbcConfig),
                        exception -> handleHeartbeatError(exception, heartbeatConnection)),
                schemaNameAdjuster);

        final OracleStreamingChangeEventSourceMetrics streamingMetrics = new OracleStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                connectorConfig);

        ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                OracleConnector.class,
                connectorConfig,
                new OracleChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext, streamingMetrics),
                new OracleChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private OracleConnection getHeartbeatConnection(OracleConnectorConfig connectorConfig, JdbcConfiguration jdbcConfig) {
        final OracleConnection connection = new OracleConnection(jdbcConfig, () -> getClass().getClassLoader());
        if (!Strings.isNullOrBlank(connectorConfig.getPdbName())) {
            connection.setSessionToPdb(connectorConfig.getPdbName());
        }
        heartbeatConnection = connection;
        return connection;
    }

    static void handleHeartbeatError(SQLException exception, JdbcConnection connection) {
        final SQLException lost = lostConnectionCause(exception);
        if (lost != null) {
            // A session Oracle has closed still answers isClosed() false, so isConnected() stays
            // true and the dead handle is reused for ever. Closing it is what lets connection()
            // re-establish on the next heartbeat.
            LOGGER.warn("Heartbeat action query lost its database connection (ORA-{}): {}. Closing it; "
                    + "the next heartbeat opens a new session.", lost.getErrorCode(), lost.getMessage());
            closeLostConnection(connection);
            return;
        }
        throw new DebeziumException("Could not execute heartbeat action query (Error: " + exception.getMessage() + ")", exception);
    }

    private static void closeLostConnection(JdbcConnection connection) {
        if (connection == null) {
            LOGGER.warn("No heartbeat connection to close; the next heartbeat creates one");
            return;
        }
        try {
            connection.close();
            LOGGER.info("Closed the lost heartbeat connection");
        }
        catch (Exception e) {
            // JdbcConnection.close() nulls its connection in a finally, so the next heartbeat
            // re-establishes regardless; an unchecked failure must not escape the error handler.
            LOGGER.warn("Could not close the lost heartbeat connection; the next heartbeat re-establishes anyway", e);
        }
    }

    /**
     * The throwable marking the connection lost, or null to let the error stay fatal. Returned
     * rather than a boolean so the log names the code and message of the one that actually
     * matched: the match is often a nested cause while the outer wrapper carries code 0.
     */
    static SQLException lostConnectionCause(SQLException exception) {
        // Both contracts say a retry can succeed; SQLNonTransientConnectionException's says it cannot
        // until the cause is corrected, so it is excluded. ORA-17002 arrives as SQLRecoverableException,
        // so connect-time failures recover too -- deliberately: whether an unreachable database stops
        // the connector is the main connection's call, not a liveness probe's.
        Throwable throwable = exception;
        while (throwable != null) {
            if (throwable instanceof SQLRecoverableException
                    || throwable instanceof SQLTransientConnectionException
                    || (throwable instanceof SQLException
                            && CONNECTION_LOST_ERROR_CODES.contains(((SQLException) throwable).getErrorCode()))) {
                return (SQLException) throwable;
            }
            throwable = throwable.getCause();
        }
        return null;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }

    private void validateRedoLogConfiguration() {
        // Check whether the archive log is enabled.
        final boolean archivelogMode = jdbcConnection.isArchiveLogMode();
        if (!archivelogMode) {
            throw new DebeziumException("The Oracle server is not configured to use a archive log LOG_MODE, which is "
                    + "required for this connector to work properly. Change the Oracle configuration to use a "
                    + "LOG_MODE=ARCHIVELOG and restart the connector.");
        }
    }

    private void validateAndLoadDatabaseHistory(OracleConnectorConfig config, OraclePartition partition, OracleOffsetContext offset, OracleDatabaseSchema schema) {
        if (offset == null) {
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                // We are in schema only recovery mode, use the existing redo log position
                // would like to also verify redo log position exists, but it defaults to 0 which is technically valid
                throw new DebeziumException("Could not find existing redo log information while attempting schema only recovery snapshot");
            }
            LOGGER.info("Connector started for the first time, database history recovery will not be executed");
            schema.initializeStorage();
            return;
        }
        if (!schema.historyExists()) {
            LOGGER.warn("Database history was not found but was expected");
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                LOGGER.info("The db-history topic is missing but we are in {} snapshot mode. " +
                        "Attempting to snapshot the current schema and then begin reading the redo log from the last recorded offset.",
                        OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            else {
                throw new DebeziumException("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to "
                        + OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            schema.initializeStorage();
            return;
        }
        schema.recover(Offsets.of(partition, offset));
    }
}
