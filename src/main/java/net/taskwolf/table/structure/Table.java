package net.taskwolf.table.structure;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.database.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class Table extends DatabaseTable {
  public static CompletableFuture<Table> create(
    DatabaseConnection connection, DatabaseKeyspace keyspace,
    BundleDatabaseTable bundleDatabaseTable, TableDatabaseTable tableDatabaseTable,
    TableEntry entry
  ) {
    var table = new Table(connection, keyspace, entry.id(), Lists.newArrayList(),
      bundleDatabaseTable, tableDatabaseTable, entry);
    return table.findTableColumns().thenAccept(table::fillColumns)
      .thenApply(value -> table);
  }

  public static Table create(
    DatabaseConnection connection, DatabaseKeyspace keyspace,
    BundleDatabaseTable bundleDatabaseTable, TableDatabaseTable tableDatabaseTable,
    List<DatabaseColumn> columns, TableEntry entry
  ) {
    return new Table(connection, keyspace, entry.id(), columns,
      bundleDatabaseTable, tableDatabaseTable, entry);
  }

  private final BundleDatabaseTable bundleDatabaseTable;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableEntry entry;

  private Table(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns, BundleDatabaseTable bundleDatabaseTable,
    TableDatabaseTable tableDatabaseTable, TableEntry entry
  ) {
    super(connection, keyspace, name, columns);
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.tableDatabaseTable = tableDatabaseTable;
    this.entry = entry;
  }

  public CompletableFuture<Boolean> insertContent(TableRow row) {
    var size = entry.size() + row.size();
    return checkDatabaseSizeLimit(size).thenApply(limitReached ->
      insertContent(row, size, limitReached));
  }

  private boolean insertContent(TableRow row, long size, boolean limitReached) {
    if (limitReached) {
      return false;
    }
    tableDatabaseTable.updateTableSize(entry, size);
    insert(DatabaseRow.of(createRowValues(row)));
    return true;
  }

  public CompletableFuture<UUID> generateAvailableContentId() {
    var futureResponse = new CompletableFuture<UUID>();
    var id = UUID.randomUUID();
    contentExists(id).thenApply(exists -> exists ?
      generateAvailableContentId().thenApply(futureResponse::complete) :
      CompletableFuture.completedFuture(futureResponse.complete(id)));
    return futureResponse;
  }

  public CompletableFuture<Boolean> updateContent(UUID id, TableRow row) {
    return selectRow(DatabaseCell.create(id))
      .thenApply(previousRow -> TableRow.of(previousRow, columns()))
      .thenApply(previousRow -> entry.size() - previousRow.size() + row.size())
      .thenCompose(size -> checkDatabaseSizeLimit(size)
        .thenApply(limitReached -> updateContent(id, row, size, limitReached)));
  }

  private boolean updateContent(
    UUID id, TableRow row, long size, boolean limitReached
  ) {
    if (limitReached) {
      return false;
    }
    tableDatabaseTable.updateTableSize(entry, size);
    update(DatabaseCell.create(id), DatabaseRow.of(createRowValues(row)));
    return true;
  }

  private CompletableFuture<Boolean> checkDatabaseSizeLimit(long size) {
    return bundleDatabaseTable.findBundle(entry.owner()).thenCompose(bundle ->
      tableDatabaseTable.findAllTablesOfOwner(entry.owner()).thenApply(tables ->
          tables.stream().filter(table -> !table.id().equals(entry.id()))
            .mapToLong(TableEntry::size).sum() + size)
        .thenApply(dataSize -> bundle.databaseDataLimit() > 0 &&
          dataSize * Math.pow(10, -9) >= bundle.databaseDataLimit()));
  }

  private Object[] createRowValues(TableRow row) {
    var cells = row.cells();
    var columns = columns();
    var values = new Object[columns.size()];
    for (var i = 0; i < columns.size(); i++) {
      var column = columns.get(i);
      values[i] = findRowValueForColumn(column, cells);
    }
    return values;
  }

  private Object findRowValueForColumn(DatabaseColumn column, List<TableCell> cells) {
    for (var cell : cells) {
      if (column.name().equalsIgnoreCase(cell.column())) {
        return cell.value();
      }
    }
    return "";
  }

  public CompletableFuture<Boolean> contentExists(UUID id) {
    return exists(DatabaseCell.create(id));
  }

  public CompletableFuture<List<TableRow>> findContent(
    int pageSize, int pageNumber
  ) {
    return selectPagesRows(pageSize, pageNumber).thenApply(rows ->
      rows.stream().map(row -> TableRow.of(row, columns())).toList());
  }

  public void removeContent(UUID id) {
    selectRow(DatabaseCell.create(id))
      .thenApply(row -> TableRow.of(row, columns()))
      .thenAccept(row -> tableDatabaseTable.updateTableSize(entry,
        entry.size() - row.size()))
      .thenAccept(value -> delete(DatabaseCell.create(id)));
  }

  private CompletableFuture<List<DatabaseColumn>> findTableColumns() {
    var query = new StringBuilder("SELECT * FROM system_schema.columns WHERE ");
    query.append("keyspace_name = '");
    query.append(keyspace().name());
    query.append("' AND table_name = '");
    query.append(name());
    query.append("';");
    var result = connection().session().executeAsync(query.toString());
    var futureResponse = new CompletableFuture<List<DatabaseColumn>>();
    result.thenAccept(resultSet ->
      futureResponse.complete(createDatabaseColumns(resultSet.currentPage())));
    return futureResponse;
  }

  private List<DatabaseColumn> createDatabaseColumns(Iterable<Row> rows) {
    DatabaseColumn primaryKeyColumn = null;
    var columns = Lists.<DatabaseColumn>newArrayList();
    for (var row : rows) {
      var column = createDatabaseColumnEntry(row);
      if (column.type().isPrimaryKey()) {
        primaryKeyColumn = column;
      } else {
        columns.add(column);
      }
    }
    columns.add(0, primaryKeyColumn);
    return columns;
  }

  private DatabaseColumn createDatabaseColumnEntry(Row row) {
    var columnName = row.getString("column_name");
    var columnType = row.getString("kind").equals("partition_key") ?
      DatabaseColumn.Type.PRIMARY_KEY : DatabaseColumn.Type.REGULAR;
    var dataType = row.getString("type").toUpperCase();
    if (dataType.contains("LIST")) {
      return DatabaseListColumn.create(columnName, DatabaseDataType.valueOf(
        dataType.replace("LIST", "").replace("<", "").replace(">", "")),
        columnType);
    }
    return DatabaseColumn.create(columnName, DatabaseDataType.valueOf(dataType),
      columnType);
  }

  @Override
  public CompletableFuture<Void> addColumn(DatabaseColumn column) {
    return super.addColumn(column).thenAccept(value -> recalculateTableSize());
  }

  @Override
  public CompletableFuture<Void> dropColumn(String columnName) {
    return super.dropColumn(columnName).thenAccept(value -> recalculateTableSize());
  }

  private void recalculateTableSize() {
    selectAllRows().thenApply(rows -> rows.stream()
        .mapToLong(row -> TableRow.of(row, columns()).size()).sum())
      .thenAccept(size -> tableDatabaseTable.updateTableSize(entry, size));
  }
}
