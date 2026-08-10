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
     */
    private final ConcurrentMap<Long, TableObjectId> objectIdToTableId = new ConcurrentHashMap<>();

    /**
     * Bounded negative-lookup cache of object ids known to be unresolvable, preventing repeated
     * database lookups for the same unknown object id (upstream DBZ-8399 semantics). Bounded by
     * {@code internal.log.mining.object.id.cache.size} (upstream DBZ-8071).
     */
    private final Map<Long, Boolean> unresolvableObjectIds;

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
        this.unresolvableObjectIds = Collections.synchronizedMap(new LinkedHashMap<Long, Boolean>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                return size() > objectIdCacheSize;
            }
        });
    }

    /**
     * Registers the Oracle object identifiers for a captured table.
     *
     * @param tableId the relational table identifier, ignored if {@code null}
     * @param objectId the table's {@code OBJECT_ID}, ignored if {@code null}
     * @param dataObjectId the table's {@code DATA_OBJECT_ID}; may be {@code null} when unknown, in
     *            which case the entry matches lookups regardless of the requested data object id
     */
    public void registerTableObjectId(TableId tableId, Long objectId, Long dataObjectId) {
        if (tableId != null && objectId != null) {
            objectIdToTableId.put(objectId, new TableObjectId(tableId, dataObjectId));
            unresolvableObjectIds.remove(objectId);
        }
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
     * Returns whether a prior lookup already failed to resolve the given object id.
     */
    public boolean isObjectIdUnresolvable(Long objectId) {
        return objectId != null && unresolvableObjectIds.containsKey(objectId);
    }

    /**
     * Records that the given object id could not be resolved, so subsequent lookups fail fast.
     */
    public void registerUnresolvableObjectId(Long objectId) {
        if (objectId != null) {
            unresolvableObjectIds.put(objectId, Boolean.TRUE);
        }
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
