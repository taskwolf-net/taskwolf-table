package net.taskwolf.table.structure;

import net.taskwolf.core.database.*;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class TableSizeDatabaseTable extends DatabaseTable {
  private static final String TABLE_NAME = "user_table_size";

  public static TableSizeDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT,
      DatabaseColumn.Type.PRIMARY_KEY));
    columns.add(DatabaseColumn.create("size", DatabaseDataType.COUNTER));
    return new TableSizeDatabaseTable(connection, keyspace, TABLE_NAME, columns);
  }

  private TableSizeDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  public CompletableFuture<Void> updateSize(
    String tableId, long sizeAddition
  ) {
    return updateCounter(tableId, DatabaseRow.of(tableId, sizeAddition));
  }

  public CompletableFuture<Void> deleteSize(String tableId) {
    return delete(tableId);
  }

  public CompletableFuture<Boolean> sizeExists(String tableId) {
    return exists(tableId);
  }

  public CompletableFuture<Long> findSize(String tableId) {
    return selectRowSecure(tableId)
      .thenApply(result -> result.map(row -> row.findCell(1).longValue()).orElse(0L));
  }
}
