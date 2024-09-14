package com.dulno.table.structure;

import com.google.common.collect.Lists;
import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseComparison;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.database.paging.DatabaseDirection;
import com.dulno.core.database.paging.DatabaseOrder;
import com.dulno.core.database.paging.DatabasePage;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class TableDatabaseTable extends DatabaseTable {
  private static final String TABLE_NAME = "user_table";

  public static TableDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("owner", DatabaseDataType.UUID,
      DatabaseColumn.Type.PARTITION_KEY));
    columns.add(DatabaseColumn.create("id", DatabaseDataType.TEXT,
      DatabaseColumn.Type.CLUSTERING_KEY));
    columns.add(DatabaseColumn.create("creator", DatabaseDataType.UUID));
    columns.add(DatabaseColumn.create("name", DatabaseDataType.TEXT));
    columns.add(DatabaseColumn.create("created", DatabaseDataType.BIGINT));
    columns.add(DatabaseColumn.create("size", DatabaseDataType.BIGINT));
    var table = new TableDatabaseTable(connection, keyspace, TABLE_NAME, columns);
    table.createIfNotExists();
    table.createIndexIfNotExists("id");
    table.createIndexIfNotExists("name",
      "'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = " +
        "{'mode': 'CONTAINS', 'analyzer_class': " +
        "'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', " +
        "'case_sensitive': 'false'}");
    table.initializeViews();
    return table;
  }

  private final Random random = new Random();
  private DatabaseTable nameView;
  private DatabaseTable creatorView;
  private DatabaseTable createdView;
  private DatabaseTable sizeView;

  private TableDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  private void initializeViews() {
    nameView = createMaterializedViewIfNotExists("name_view", "name");
    creatorView = createMaterializedViewIfNotExists("creator_view", "creator");
    createdView = createMaterializedViewIfNotExists("created_view", "created");
    sizeView = createMaterializedViewIfNotExists("size_view", "size");
  }

  public void insertTable(TableEntry table) {
    insertTable(table.owner(), table.id(), table.creator(), table.name(),
      table.created(), table.size());
  }

  public void insertTable(
    UUID owner, String id, UUID creator, String name, long created, long size
  ) {
    insert(DatabaseRow.of(owner, id, creator, name, created, size));
  }

  public void changeTableName(String id, String name) {
    findTable(id).thenAccept(table -> changeTableName(table, name));
  }

  public void changeTableName(TableEntry entry, String name) {
    entry.changeName(name);
    updateTable(entry);
  }

  public void updateTableSize(String id, long size) {
    findTable(id).thenAccept(table -> updateTableSize(table, size));
  }

  public void updateTableSize(TableEntry entry, long size) {
    entry.updateSize(size);
    updateTable(entry);
  }

  private void updateTable(TableEntry entry) {
    update(DatabaseCondition.of("owner", entry.owner(), "id", entry.id()),
      DatabaseRow.of(entry.owner(), entry.id(), entry.creator(), entry.name(),
        entry.created(), entry.size()));
  }

  public void deleteTable(String tableId) {
    findTable(tableId).thenAccept(table ->
      delete(DatabaseCondition.of("owner", table.owner(), "id", table.id())));
  }

  public CompletableFuture<String> generateAvailableTableId() {
    var futureResponse = new CompletableFuture<String>();
    var id = createTableId();
    tableExists(id).thenApply(exists -> exists ?
      generateAvailableTableId().thenApply(futureResponse::complete) :
      CompletableFuture.completedFuture(futureResponse.complete(id)));
    return futureResponse;
  }

  private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz";

  private String createTableId() {
    var value = new StringBuilder();
    for (int i = 0; i < 32; i++) {
      value.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
    }
    return value.toString();
  }

  public CompletableFuture<Boolean> tableExists(String tableId) {
    return exists(DatabaseCondition.of("id", tableId));
  }

  public CompletableFuture<TableEntry> findTable(String tableId) {
    return selectRow(DatabaseCondition.of("id", tableId)).thenApply(row ->
      TableEntry.of(row, this));
  }

  private static final int PAGE_SIZE = 5;

  public CompletableFuture<DatabasePage<TableEntry>> findTablesOfOwner(
    UUID ownerId, int targetPage, String sortingColumn, DatabaseOrder sortingOrder,
    String search, UUID creatorId, long startTime, long endTime,
    long minimumSize, long maximumSize
  ) {
    if (!search.isEmpty()) {
      var condition = DatabaseCondition.of(DatabaseComparison.create("owner", ownerId),
        DatabaseComparison.create("name", "%" + search + "%", DatabaseComparison.Type.LIKE));
      return selectRows(condition, PAGE_SIZE)
        .thenApply(rows -> createTablePage(DatabasePage.create(rows, "", 1), this));
    }
    var view = findTargetView(sortingColumn);
    return view.selectPage(ownerId, createTableConditions(creatorId, startTime,
        endTime, minimumSize, maximumSize), sortingOrder, PAGE_SIZE, targetPage)
      .thenApply(page -> createTablePage(page, view));
  }

  public CompletableFuture<DatabasePage<TableEntry>> findTablesOfOwner(
    UUID ownerId, String pageState, DatabaseDirection startingPoint,
    DatabaseDirection direction, String sortingColumn, DatabaseOrder sortingOrder,
    UUID creatorId, long startTime, long endTime, long minimumSize,
    long maximumSize
  ) {
    var view = findTargetView(sortingColumn);
    return view.shiftPage(ownerId, createTableConditions(creatorId, startTime,
        endTime, minimumSize, maximumSize), sortingOrder, PAGE_SIZE, pageState,
        startingPoint, direction)
      .thenApply(page -> createTablePage(page, view));
  }

  private DatabaseTable findTargetView(String sortingColumn) {
    if (sortingColumn.equals("name")) {
      return nameView;
    } else if (sortingColumn.equals("creator")) {
      return creatorView;
    } else if (sortingColumn.equals("created")) {
      return createdView;
    } else if (sortingColumn.equals("size")) {
      return sizeView;
    }
    return null;
  }

  private DatabaseCondition createTableConditions(
    UUID creatorId, long startTime, long endTime, long minimumSize,
    long maximumSize
  ) {
    var comparisons = Lists.<DatabaseComparison>newArrayList();
    if (creatorId != null) {
      comparisons.add(DatabaseComparison.create("creator", creatorId));
    }
    if (startTime > 0) {
      comparisons.add(DatabaseComparison.create("created", startTime,
        DatabaseComparison.Type.GREATER_EQUALS));
    }
    if (endTime > 0) {
      comparisons.add(DatabaseComparison.create("created", endTime,
        DatabaseComparison.Type.SMALLER_EQUALS));
    }
    if (minimumSize > 0) {
      comparisons.add(DatabaseComparison.create("size", minimumSize,
        DatabaseComparison.Type.GREATER_EQUALS));
    }
    if (maximumSize > 0) {
      comparisons.add(DatabaseComparison.create("size", maximumSize,
        DatabaseComparison.Type.SMALLER_EQUALS));
    }
    return DatabaseCondition.create(comparisons);
  }

  private DatabasePage<TableEntry> createTablePage(
    DatabasePage<DatabaseRow> page, DatabaseTable table
  ) {
    return DatabasePage.create(
      page.content().stream().map(row -> TableEntry.of(row, table)).toList(),
      page.pageState(), page.pageNumber());
  }

  public CompletableFuture<Long> findTableCount(UUID ownerId) {
    return count(DatabaseCondition.of("owner", ownerId));
  }

  public CompletableFuture<List<TableEntry>> findAllTablesOfOwner(UUID ownerId) {
    return selectRows(DatabaseCondition.of("owner", ownerId)).thenApply(rows ->
      rows.stream().map(row -> TableEntry.of(row, this)).toList());
  }
}
