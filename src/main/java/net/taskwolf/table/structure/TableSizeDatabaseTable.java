package net.taskwolf.table.structure;

import com.google.common.collect.Lists;
import net.taskwolf.core.database.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class TableSizeDatabaseTable extends DatabaseTable {
  private static final String TABLE_NAME = "user_table_size";

  public static TableSizeDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("target", DatabaseDataType.UUID,
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
    UUID targetId, long sizeAddition
  ) {
    var query = new StringBuilder();
    query.append("size");
    query.append(sizeAddition >= 0 ? "+" : "-");
    query.append(Math.abs(sizeAddition));
    return update(targetId, DatabaseRow.of(targetId, query));
  }

  public CompletableFuture<Void> deleteSize(UUID targetId) {
    return delete(targetId);
  }

  public CompletableFuture<Boolean> sizeExists(UUID targetId) {
    return exists(targetId);
  }

  public CompletableFuture<Long> findSize(UUID targetId) {
    return selectRowSecure(targetId)
      .thenApply(result -> result.map(row -> row.findCell(1).longValue()).orElse(0L));
  }
}
