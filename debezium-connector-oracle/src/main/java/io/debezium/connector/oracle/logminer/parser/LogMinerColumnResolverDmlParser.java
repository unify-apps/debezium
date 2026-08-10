/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

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
 * LogMiner numbers the {@code COL x} placeholders by the column's physical position, skipping any
 * generated (virtual) columns; the relational model's column positions include them. The mapping is
 * therefore computed per table and cached, and the cache entry is invalidated by the event processor
 * whenever a schema change for the table is observed.
 * <p>
 * Backport of upstream DBZ-3401 ({@code 648db8886}) in its final form after DBZ-8597
 * ({@code 5909410c1}), adapted to the 1.9.8 no-argument parser constructor.
 *
 * @author Chris Cranford
 */
public class LogMinerColumnResolverDmlParser extends LogMinerDmlParser {

    private static final Pattern COLUMN_PLACEHOLDER_PATTERN = Pattern.compile("COL \\d*");

    private final Map<TableId, Map<String, Integer>> tableColumnPositionCache = new HashMap<>();

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
}
