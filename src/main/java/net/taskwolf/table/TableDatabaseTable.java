package net.taskwolf.table;

import com.google.common.collect.Lists;
import net.taskwolf.core.database.*;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class TableDatabaseTable extends DatabaseTable {
  private static final String TABLE_NAME = "table";

  public static TableDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("id", DatabaseDataType.UUID,
      DatabaseColumn.Type.PRIMARY_KEY));
    columns.add(DatabaseColumn.create("owner", DatabaseDataType.UUID));
    columns.add(DatabaseColumn.create("creator", DatabaseDataType.UUID));
    columns.add(DatabaseColumn.create("name", DatabaseDataType.TEXT));
    columns.add(DatabaseColumn.create("created", DatabaseDataType.BIGINT));
    return new TableDatabaseTable(connection, keyspace, TABLE_NAME, columns);
  }

  private TableDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns
  ) {
    super(connection, keyspace, name, columns);
  }

  public void insertTable(TableEntry table) {
    insertTable(table.id(), table.owner(), table.creator(), table.name(),
      table.created());
  }

  public void insertTable(
    UUID id, UUID owner, UUID creator, String name, long created
  ) {
    insert(DatabaseRow.of(id, owner, creator, name, created));
  }

  public void changeTableName(UUID id, String name) {
    findTable(id).thenAccept(table -> changeTableName(table, name));
  }

  private void changeTableName(TableEntry entry, String name) {
    entry.changeName(name);
    updateTable(entry);
  }

  private void updateTable(TableEntry entry) {
    update(DatabaseCell.create(entry.id()), DatabaseRow.of(entry.id()));
  }

  public CompletableFuture<Boolean> tableExists(UUID id) {
    return exists(DatabaseCell.create(id));
  }

  public void deleteTable(UUID id) {
    delete(DatabaseCell.create(id));
  }

  public CompletableFuture<List<TableEntry>> findTablesOfOwner(UUID ownerId) {
    return selectRows("owner=" + ownerId  + " ALLOW FILTERING").thenApply(rows ->
      rows.stream().map(TableEntry::of).collect(Collectors.toList()));
  }

  public CompletableFuture<TableEntry> findTable(UUID id) {
    return selectRow(DatabaseCell.create(id)).thenApply(TableEntry::of);
  }
}
