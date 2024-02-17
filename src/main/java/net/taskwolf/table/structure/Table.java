package net.taskwolf.table.structure;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import net.taskwolf.core.database.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class Table extends DatabaseTable {
  public static CompletableFuture<Table> create(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String id
  ) {
    var table = new Table(connection, keyspace, id, Lists.newArrayList());
    return table.findTableColumns().thenAccept(table::fillColumns)
      .thenApply(value -> table);
  }

  public static Table create(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String id,
    List<DatabaseColumn> columns
  ) {
    return new Table(connection, keyspace, id, columns);
  }

  private Table(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  public void insertContent(TableRow row) {
    var cells = row.cells();
    var values = new Object[row.cells().size()];
    for (var i = 0; i < cells.size(); i++) {
      values[i] = cells.get(i).value();
    }
    insert(DatabaseRow.of(values));
  }

  public CompletableFuture<UUID> generateAvailableContentId() {
    var futureResponse = new CompletableFuture<UUID>();
    var id = UUID.randomUUID();
    contentExists(id).thenApply(exists -> exists ?
      generateAvailableContentId().thenApply(futureResponse::complete) :
      CompletableFuture.completedFuture(futureResponse.complete(id)));
    return futureResponse;
  }

  public void updateContent(UUID id, TableRow row) {
    var cells = row.cells();
    var values = new Object[row.cells().size()];
    for (var i = 0; i < cells.size(); i++) {
      values[i] = cells.get(i).value();
    }
    update(DatabaseCell.create(id), DatabaseRow.of(values));
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
    delete(DatabaseCell.create(id));
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
}
