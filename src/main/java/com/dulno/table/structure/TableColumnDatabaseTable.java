package com.dulno.table.structure;

import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public final class TableColumnDatabaseTable extends DatabaseTable {
  private static final String TABLE_NAME = "user_table_column";

  public static TableColumnDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("id", DatabaseDataType.TEXT,
      DatabaseColumn.Type.PARTITION_KEY));
    columns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    columns.add(DatabaseColumn.create("type", DatabaseDataType.TEXT));
    columns.add(DatabaseColumn.create("name", DatabaseDataType.TEXT));
    var table = new TableColumnDatabaseTable(connection, keyspace, TABLE_NAME,
      columns);
    table.createIfNotExists();
    table.initializeViews();
    return table;
  }

  private final Random random = new Random();
  private DatabaseTable tableView;

  private TableColumnDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  private void initializeViews() {
    tableView = createMaterializedViewIfNotExists("table_view", "tableId",
      DatabaseColumn.Type.PARTITION_KEY);
  }

  public CompletableFuture<Void> insertColumn(TableColumn column) {
    return insertColumn(column.id(), column.tableId(), column.type(), column.name());
  }

  public CompletableFuture<Void> insertColumn(
    String id, String tableId, TableColumnType type, String name
  ) {
    return insert(DatabaseRow.of(id, tableId, type.toString(), name));
  }

  public CompletableFuture<Void> changeColumnName(String id, String name) {
    return findColumn(id).thenCompose(column -> changeColumnName(column, name));
  }

  public CompletableFuture<Void> changeColumnName(TableColumn column, String name) {
    column.changeName(name);
    return updateColumn(column);
  }

  private CompletableFuture<Void> updateColumn(TableColumn column) {
    return update(DatabaseCondition.of("id", column.id()),
      DatabaseRow.of(column.id(), column.tableId(), column.type().toString(),
        column.name()));
  }

  public CompletableFuture<Void> deleteColumn(String id) {
    return delete(DatabaseCondition.of("id", id));
  }

  public CompletableFuture<String> generateAvailableColumnId() {
    var futureResponse = new CompletableFuture<String>();
    var id = createColumnId();
    columnExists(id).thenApply(exists -> exists ?
      generateAvailableColumnId().thenApply(futureResponse::complete) :
      CompletableFuture.completedFuture(futureResponse.complete(id)));
    return futureResponse;
  }

  private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz";

  private String createColumnId() {
    var value = new StringBuilder();
    for (int i = 0; i < 32; i++) {
      value.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
    }
    return value.toString();
  }

  public CompletableFuture<Boolean> columnExists(String id) {
    return exists(DatabaseCondition.of("id", id));
  }

  public CompletableFuture<TableColumn> findColumn(String id) {
    return selectRow(DatabaseCondition.of("id", id))
      .thenApply(row -> TableColumn.of(row, this));
  }

  public CompletableFuture<List<TableColumn>> findTableColumns(String tableId) {
    return tableView.selectRows(DatabaseCondition.of("tableId", tableId))
      .thenApply(rows -> rows.stream().map(row -> TableColumn.of(row, tableView))
        .toList());
  }
}