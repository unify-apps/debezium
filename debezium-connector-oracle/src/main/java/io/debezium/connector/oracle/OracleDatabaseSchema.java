/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import oracle.jdbc.OracleTypes;

/**
 * The schema of an Oracle database.
 *
 * @author Gunnar Morling
 */
public class OracleDatabaseSchema extends HistorizedRelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseSchema.class);

    private final OracleDdlParser ddlParser;
    private final ConcurrentMap<TableId, List<Column>> lobColumnsByTableId = new ConcurrentHashMap<>();
    private final OracleValueConverters valueConverters;

    /**
     * Registry of Oracle {@code OBJECT_ID} values to relational table identifiers, used by the LogMiner
     * hybrid mining strategy to resolve events whose table name could not be reconstructed by LogMiner
     * (reported as {@code OBJ# <n>} or {@code UNKNOWN} for dropped and purged objects).
     * <p>
     * Upstream stores these identifiers as relational model {@link Table} attributes (DBZ-3401) and
     * resolves them through a cache in this class (DBZ-8071, DBZ-8925). Debezium core 1.9.8 has no
     * table attribute support, so this registry is the system of record instead: it is warmed from
     * {@code ALL_OBJECTS} for all captured tables when streaming starts, updated as DDL events are
     * observed, and consulted on demand. Entries are intentionally never removed on DROP so trailing
     * DML events that precede the drop in the redo stream still resolve.
     * <p>
     * Unbounded, and only safe as such because registration rejects anything outside the capture set;
     * a miss therefore means "not a captured table", which is what the event processor relies on.
     */
    private final ConcurrentMap<Long, TableObjectId> objectIdToTableId = new ConcurrentHashMap<>();

    /**
     * Object ids not worth looking up again: unresolvable, or resolved outside the capture set
     * (upstream DBZ-8399, extended to the second case). Caching the decision rather than the table id
     * is what keeps this bounded by {@code internal.log.mining.object.id.cache.size}.
     */
    private final Map<Long, ObjectIdSkipReason> skippedObjectIds;

    /**
     * Captured tables with no registered object id, so the registry cannot be treated as covering the
     * whole capture set. Only populated when a captured table is absent from {@code ALL_OBJECTS} at
     * streaming start, which a drop-and-purge before the connector caught up produces.
     */
    private final Set<TableId> capturedTablesWithoutObjectId = ConcurrentHashMap.newKeySet();

    private boolean storageInitializationExecuted = false;

    public OracleDatabaseSchema(OracleConnectorConfig connectorConfig, OracleValueConverters valueConverters,
                                DefaultValueConverter defaultValueConverter, SchemaNameAdjuster schemaNameAdjuster,
                                TopicSelector<TableId> topicSelector, TableNameCaseSensitivity tableNameCaseSensitivity) {
        super(connectorConfig, topicSelector, connectorConfig.getTableFilters().dataCollectionFilter(),
                connectorConfig.getColumnFilter(),
                new TableSchemaBuilder(
                        valueConverters,
                        defaultValueConverter,
                        schemaNameAdjuster,
                        connectorConfig.customConverterRegistry(),
                        connectorConfig.getSourceInfoStructMaker().schema(),
                        connectorConfig.getSanitizeFieldNames(),
                        false),
                TableNameCaseSensitivity.INSENSITIVE.equals(tableNameCaseSensitivity),
                connectorConfig.getKeyMapper());

        this.valueConverters = valueConverters;
        this.ddlParser = new OracleDdlParser(
                true,
                false,
                connectorConfig.isSchemaCommentsHistoryEnabled(),
                valueConverters,
                connectorConfig.getTableFilters().dataCollectionFilter());

        final int objectIdCacheSize = connectorConfig.getLogMiningObjectIdCacheSize();
        this.skippedObjectIds = Collections.synchronizedMap(new LinkedHashMap<Long, ObjectIdSkipReason>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, ObjectIdSkipReason> eldest) {
                return size() > objectIdCacheSize;
            }
        });
    }

    /**
     * Registers the Oracle object identifiers for a captured table.
     * <p>
     * A table outside the capture set is rejected rather than stored, keeping the unbounded registry
     * sized by the capture set.
     *
     * @param tableId the relational table identifier, ignored if {@code null}
     * @param objectId the table's {@code OBJECT_ID}, ignored if {@code null}
     * @param dataObjectId the table's {@code DATA_OBJECT_ID}; may be {@code null} when unknown, in
     *            which case the entry matches lookups regardless of the requested data object id
     * @return {@code true} if the identifiers were registered, {@code false} if the table is not
     *         captured by this connector and was therefore not stored
     */
    public boolean registerTableObjectId(TableId tableId, Long objectId, Long dataObjectId) {
        if (tableId == null || objectId == null) {
            return false;
        }
        if (!isCapturedTable(tableId)) {
            // Object ids are unique, so naming this one outside the capture set means any mapping it
            // still has - a rename out of the capture set leaves one - no longer describes it.
            objectIdToTableId.remove(objectId);
            return false;
        }
        objectIdToTableId.put(objectId, new TableObjectId(tableId, dataObjectId));
        skippedObjectIds.remove(objectId);
        capturedTablesWithoutObjectId.remove(tableId);
        return true;
    }

    /**
     * Records that a captured table has no resolvable object id, so an unresolvable event can no
     * longer be presumed to belong to another connector's tables.
     *
     * @param tableId the captured table, ignored if {@code null}
     */
    public void registerCapturedTableWithoutObjectId(TableId tableId) {
        if (tableId != null) {
            capturedTablesWithoutObjectId.add(tableId);
        }
    }

    /**
     * @return {@code true} when every captured table has a registered object id, which is what makes
     *         a registry miss proof that the object is not captured
     */
    public boolean isObjectIdRegistryComplete() {
        return capturedTablesWithoutObjectId.isEmpty();
    }

    /**
     * @return the captured tables with no registered object id, for diagnostics
     */
    public Set<TableId> getCapturedTablesWithoutObjectId() {
        return Collections.unmodifiableSet(capturedTablesWithoutObjectId);
    }

    /**
     * @param tableId the table identifier, may be {@code null}
     * @return {@code true} when the table matches the configured include/exclude lists
     */
    public boolean isCapturedTable(TableId tableId) {
        return tableId != null && getTableFilter().isIncluded(tableId);
    }

    /**
     * Get the {@link TableId} by the table's Oracle object id.
     *
     * @param objectId the object id to look up, must not be {@code null}
     * @param dataObjectId the data object id, may be {@code null} to match on object id alone
     * @return the resolved table identifier, or {@code null} if no registered entry matches
     */
    public TableId getTableIdByObjectId(Long objectId, Long dataObjectId) {
        Objects.requireNonNull(objectId, "The database table object id must not be null");
        final TableObjectId entry = objectIdToTableId.get(objectId);
        if (entry == null) {
            return null;
        }
        if (dataObjectId != null && entry.dataObjectId != null && !dataObjectId.equals(entry.dataObjectId)) {
            return null;
        }
        return entry.tableId;
    }

    /**
     * Reads through the access-ordered cache, so a hit refreshes the entry's recency.
     *
     * @param objectId the object id to look up, may be {@code null}
     * @return the recorded reason, or {@code null} if this object id has not been decided
     */
    public ObjectIdSkipReason getObjectIdSkipReason(Long objectId) {
        return objectId == null ? null : skippedObjectIds.get(objectId);
    }

    /**
     * Records that events for the given object id must be skipped.
     *
     * @param objectId the object id, ignored if {@code null}
     * @param reason why events for it are skipped, must not be {@code null}
     */
    public void registerSkippedObjectId(Long objectId, ObjectIdSkipReason reason) {
        Objects.requireNonNull(reason, "A skip reason must be provided");
        if (objectId != null) {
            skippedObjectIds.put(objectId, reason);
        }
    }

    /** Distinguished only so that a routine skip does not log like a diagnosable one. */
    public enum ObjectIdSkipReason {
        /** Resolved to a table outside the capture set. */
        NOT_CAPTURED,

        /** Absent from both the registry and {@code ALL_OBJECTS}, so dropped and purged. */
        UNRESOLVABLE
    }

    private static final class TableObjectId {
        private final TableId tableId;
        private final Long dataObjectId;

        private TableObjectId(TableId tableId, Long dataObjectId) {
            this.tableId = tableId;
            this.dataObjectId = dataObjectId;
        }
    }

    public Tables getTables() {
        return tables();
    }

    public OracleValueConverters getValueConverters() {
        return valueConverters;
    }

    @Override
    public OracleDdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> {
                    buildAndRegisterSchema(x.getTable());
                    tables().overwriteTable(x.getTable());
                });
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        if (!databaseHistory.storeOnlyCapturedTables() ||
                schemaChange.getTables().stream().map(Table::id).anyMatch(getTableFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        storageInitializationExecuted = true;
    }

    public boolean isStorageInitializationExecuted() {
        return storageInitializationExecuted;
    }

    /**
     * Return true if the database history entity exists
     */
    public boolean historyExists() {
        return databaseHistory.exists();
    }

    @Override
    protected void removeSchema(TableId id) {
        super.removeSchema(id);
        lobColumnsByTableId.remove(id);
    }

    @Override
    protected void buildAndRegisterSchema(Table table) {
        if (getTableFilter().isIncluded(table.id())) {
            super.buildAndRegisterSchema(table);

            // Cache LOB column mappings for performance
            buildAndRegisterTableLobColumns(table);
        }
    }

    /**
     * Get a list of large object (LOB) columns for the specified relational table identifier.
     *
     * @param id the relational table identifier
     * @return a list of LOB columns, may be empty if the table has no LOB columns
     */
    public List<Column> getLobColumnsForTable(TableId id) {
        return lobColumnsByTableId.getOrDefault(id, Collections.emptyList());
    }

    /**
     * Returns whether the specified value is the unavailable value placeholder for an LOB column.
     */
    public boolean isColumnUnavailableValuePlaceholder(Column column, Object value) {
        if (isClobColumn(column)) {
            return valueConverters.getUnavailableValuePlaceholderString().equals(value);
        }
        else if (isBlobColumn(column)) {
            return ByteBuffer.wrap(valueConverters.getUnavailableValuePlaceholderBinary()).equals(value);
        }
        return false;
    }

    /**
     * Return whether the provided relational column model is a LOB data type.
     */
    public static boolean isLobColumn(Column column) {
        return isClobColumn(column) || isBlobColumn(column);
    }

    /**
     * Returns whether the provided relational column model is a CLOB or NCLOB data type.
     */
    private static boolean isClobColumn(Column column) {
        return column.jdbcType() == OracleTypes.CLOB || column.jdbcType() == OracleTypes.NCLOB;
    }

    /**
     * Returns whether the provided relational column model is a CLOB data type.
     */
    private static boolean isBlobColumn(Column column) {
        return column.jdbcType() == OracleTypes.BLOB;
    }

    private void buildAndRegisterTableLobColumns(Table table) {
        final List<Column> lobColumns = new ArrayList<>();
        for (Column column : table.columns()) {
            switch (column.jdbcType()) {
                case OracleTypes.CLOB:
                case OracleTypes.NCLOB:
                case OracleTypes.BLOB:
                    lobColumns.add(column);
                    break;
            }
        }
        if (!lobColumns.isEmpty()) {
            lobColumnsByTableId.put(table.id(), lobColumns);
        }
        else {
            lobColumnsByTableId.remove(table.id());
        }
    }
}
