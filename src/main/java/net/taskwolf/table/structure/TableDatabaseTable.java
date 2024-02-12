package net.taskwolf.table.structure;

import com.google.common.collect.Lists;
import net.taskwolf.core.database.*;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class TableDatabaseTable extends DatabaseTable {
  private static final String TABLE_NAME = "user_table";

  public static TableDatabaseTable create(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("id", DatabaseDataType.TEXT,
      DatabaseColumn.Type.PRIMARY_KEY));
    columns.add(DatabaseColumn.create("owner", DatabaseDataType.UUID));
    columns.add(DatabaseColumn.create("creator", DatabaseDataType.UUID));
    columns.add(DatabaseColumn.create("name", DatabaseDataType.TEXT));
    columns.add(DatabaseColumn.create("created", DatabaseDataType.BIGINT));
    return new TableDatabaseTable(connection, keyspace, TABLE_NAME, columns);
  }

  private final Random random = new Random();

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
    String id, UUID owner, UUID creator, String name, long created
  ) {
    insert(DatabaseRow.of(id, owner, creator, name, created));
  }

  public void changeTableName(String id, String name) {
    findTable(id).thenAccept(table -> changeTableName(table, name));
  }

  private void changeTableName(TableEntry entry, String name) {
    entry.changeName(name);
    updateTable(entry);
  }

  private void updateTable(TableEntry entry) {
    update(DatabaseCell.create(entry.id()), DatabaseRow.of(entry.id(),
      entry.owner(), entry.creator(), entry.name(), entry.created()));
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

  public CompletableFuture<Boolean> tableExists(String id) {
    return exists(DatabaseCell.create(id));
  }

  public void deleteTable(String id) {
    delete(DatabaseCell.create(id));
  }

  public CompletableFuture<List<TableEntry>> findTablesOfOwner(UUID ownerId) {
    return selectRows("owner=" + ownerId  + " ALLOW FILTERING").thenApply(rows ->
      rows.stream().map(TableEntry::of).collect(Collectors.toList()));
  }

  public CompletableFuture<TableEntry> findTable(String id) {
    return selectRow(DatabaseCell.create(id)).thenApply(TableEntry::of);
  }
}
