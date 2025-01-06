package com.dulno.table.structure;

import com.google.common.collect.Lists;
import com.dulno.core.database.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class TableUsageDatabaseTable extends DatabaseTable {
  private static final String TABLE_NAME = "user_table_usage";

  public static TableUsageDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("target", DatabaseDataType.UUID,
      DatabaseColumn.Type.PRIMARY_KEY));
    columns.add(DatabaseColumn.create("usage", DatabaseDataType.COUNTER));
    return new TableUsageDatabaseTable(connection, keyspace, TABLE_NAME, columns);
  }

  private TableUsageDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  public CompletableFuture<Void> updateUsage(
    UUID targetId, long usageAddition
  ) {
    return updateCounter(targetId, DatabaseRow.of(targetId, usageAddition));
  }

  public CompletableFuture<Void> deleteUsage(UUID targetId) {
    return delete(targetId);
  }

  public CompletableFuture<Boolean> usageExists(UUID targetId) {
    return exists(targetId);
  }

  public CompletableFuture<Long> findUsage(UUID targetId) {
    return selectRowSecure(targetId)
      .thenApply(result -> result.map(row -> row.findCell(1).longValue()).orElse(0L));
  }
}
