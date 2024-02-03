package net.taskwolf.table.structure;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import net.taskwolf.core.database.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class Table extends DatabaseTable {
  public static CompletableFuture<Table> create(
    DatabaseConnection connection, DatabaseKeyspace keyspace, UUID id
  ) {
    var table = new Table(connection, keyspace, id.toString(),
      Lists.newArrayList());
    return table.findTableColumns().thenAccept(table::fillColumns)
      .thenApply(value -> table);
  }

  private Table(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  public CompletableFuture<TableRow> findContent(int limit, int page) {
    //TODO: TO BE IMPLEMENTED
    return null;
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
    columns.addFirst(primaryKeyColumn);
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
