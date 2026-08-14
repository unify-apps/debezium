/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * A specialized implementation of {@link LogMinerDmlParser} that aims to map the column names when a
 * SQL reconstruction by LogMiner fails from {@code COL x} format to the actual column name using the
 * relational table model.
 * <p>
 * This implementation is only used when detecting a SQL reconstruction failure to minimize the total
 * parser overhead to be applied only in this circumstance.
 * <p>
 * LogMiner numbers the {@code COL x} placeholders by the column's stored-segment position
 * ({@code ALL_TAB_COLS.SEGMENT_COLUMN_ID}). That numbering excludes virtual columns but includes
 * hidden stored columns, such as the {@code SYS_NC…$} bitmap column Oracle 12c+ appends to a table
 * on the first fast {@code ALTER TABLE … ADD (… DEFAULT …)}; the relational model knows neither.
 * When a stored-column layout provider is {@linkplain #setStoredColumnLayoutProvider(Function)
 * registered}, the mapping is computed from the physical layout so that hidden column values are
 * skipped rather than shifting every subsequent value by one; without a provider the mapping falls
 * back to the purely positional model arithmetic. The mapping is computed per table and cached, and
 * the cache entry is invalidated by the event processor whenever a schema change for the table is
 * observed.
 * <p>
 * Backport of upstream DBZ-3401 ({@code 648db8886}) in its final form after DBZ-8597
 * ({@code 5909410c1}), adapted to the 1.9.8 no-argument parser constructor, with the stored-segment
 * mapping added on top (the upstream parser maps placeholders positionally and mis-aligns tables
 * that carry hidden stored columns).
 *
 * @author Chris Cranford
 */
public class LogMinerColumnResolverDmlParser extends LogMinerDmlParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerColumnResolverDmlParser.class);

    private static final Pattern COLUMN_PLACEHOLDER_PATTERN = Pattern.compile("COL \\d*");

    /**
     * Returned by {@link #getColumnIndexByName(String, Table)} for a {@code COL x} placeholder that
     * refers to a stored column that must not be emitted, i.e. a hidden column that has no
     * counterpart in the relational model. Callers in {@link LogMinerDmlParser} skip the value
     * assignment for this position.
     */
    public static final int SKIP_COLUMN_POSITION = -1;

    private final Map<TableId, Map<String, Integer>> tableColumnPositionCache = new HashMap<>();

    private Function<TableId, List<StoredColumn>> storedColumnLayoutProvider;

    /**
     * A physically stored table column: its name, its 1-based stored-segment position and whether
     * it is a hidden column.
     */
    public static final class StoredColumn {

        private final String name;
        private final int segmentId;
        private final boolean hidden;

        public StoredColumn(String name, int segmentId, boolean hidden) {
            this.name = name;
            this.segmentId = segmentId;
            this.hidden = hidden;
        }

        public String getName() {
            return name;
        }

        public int getSegmentId() {
            return segmentId;
        }

        public boolean isHidden() {
            return hidden;
        }
    }

    /**
     * Registers the provider of a table's physical stored-column layout, i.e. the columns that have
     * a {@code SEGMENT_COLUMN_ID}, ordered by it, including hidden stored columns. The provider may
     * return {@code null} or an empty list when the layout cannot be obtained; the purely positional
     * model mapping is then used as a fallback.
     *
     * @param storedColumnLayoutProvider the layout provider, may be {@code null}
     */
    public void setStoredColumnLayoutProvider(Function<TableId, List<StoredColumn>> storedColumnLayoutProvider) {
        this.storedColumnLayoutProvider = storedColumnLayoutProvider;
    }

    @Override
    protected int getColumnIndexByName(String columnName, Table table) {
        if (COLUMN_PLACEHOLDER_PATTERN.matcher(columnName).matches()) {
            Map<String, Integer> tableColumnPositions = tableColumnPositionCache.get(table.id());
            if (tableColumnPositions == null || !tableColumnPositions.containsKey(columnName)) {
                tableColumnPositions = updateTableColumnPositionCache(table);
            }

            final Integer position = tableColumnPositions.get(columnName);
            if (position == null) {
                throw new DebeziumException("Failed to find column " + columnName + " in table column position cache");
            }

            if (position == SKIP_COLUMN_POSITION) {
                LOGGER.trace("Skipping hidden stored column placeholder {} for table {}", columnName, table.id());
            }
            return position;
        }
        return super.getColumnIndexByName(columnName, table);
    }

    /**
     * Removes the given table from the internal table column position cache.
     *
     * @param tableId the table identifier to remove, should not be {@code null}
     */
    public void removeTableFromCache(TableId tableId) {
        tableColumnPositionCache.remove(tableId);
    }

    /**
     * Update the table cache position for the given table.
     *
     * @param table the relational table, should not be {@code null}
     * @return map of table column positions
     */
    private Map<String, Integer> updateTableColumnPositionCache(Table table) {
        if (storedColumnLayoutProvider != null) {
            final List<StoredColumn> layout = storedColumnLayoutProvider.apply(table.id());
            if (layout != null && !layout.isEmpty()) {
                return updateTableColumnPositionCacheFromStoredLayout(table, layout);
            }
            LOGGER.warn("No stored-column layout available for table {}; using positional COL-x mapping", table.id());
        }
        final Map<String, Integer> tableColumnPositions = new HashMap<>();
        int generatedColumns = 0;
        for (Column column : table.columns()) {
            if (column.isGenerated()) {
                generatedColumns++;
                continue;
            }
            final String columnName = String.format("COL %d", column.position() - generatedColumns);
            tableColumnPositions.put(columnName, column.position() - 1);
        }
        tableColumnPositionCache.put(table.id(), tableColumnPositions);
        return tableColumnPositions;
    }

    /**
     * Computes the {@code COL x} placeholder mapping from the table's physical stored-column
     * layout, which is the numbering LogMiner actually uses. Stored columns that exist in the
     * relational model map to the model column's index; hidden stored columns map to
     * {@link #SKIP_COLUMN_POSITION} so that their values, such as the fast-{@code ADD COLUMN}
     * bitmap, are discarded instead of being written into a neighboring column. Virtual columns
     * have no stored segment and therefore never appear as a placeholder.
     *
     * @param table the relational table, should not be {@code null}
     * @param layout the stored-column layout in segment order, should not be {@code null} or empty
     * @return map of table column positions
     */
    private Map<String, Integer> updateTableColumnPositionCacheFromStoredLayout(Table table, List<StoredColumn> layout) {
        final Map<String, Integer> modelPositions = new HashMap<>();
        for (Column column : table.columns()) {
            modelPositions.put(column.name().toUpperCase(), column.position() - 1);
        }

        final Map<String, Integer> tableColumnPositions = new HashMap<>();
        int skippedColumns = 0;
        for (StoredColumn stored : layout) {
            final Integer modelIndex = stored.isHidden() ? null : modelPositions.get(stored.getName().toUpperCase());
            if (modelIndex == null) {
                skippedColumns++;
            }
            tableColumnPositions.put(String.format("COL %d", stored.getSegmentId()),
                    modelIndex != null ? modelIndex : SKIP_COLUMN_POSITION);
        }

        tableColumnPositionCache.put(table.id(), tableColumnPositions);
        LOGGER.info("Using stored-column layout for COL-x mapping of table {}: {} stored columns, {} skipped",
                table.id(), layout.size(), skippedColumns);
        LOGGER.debug("Stored-column COL-x mapping for table {}: {}", table.id(), tableColumnPositions);
        return tableColumnPositions;
    }
}
