package net.taskwolf.table.structure;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Singleton
public final class TableFactory {
  private final DatabaseConnection tableConnection;
  private final DatabaseKeyspace tableKeyspace;
  private final TableDatabaseTable tableDatabaseTable;

  @Inject
  private TableFactory(
    @Named("tableConnection") DatabaseConnection tableConnection,
    @Named("tableKeyspace") DatabaseKeyspace tableKeyspace,
    TableDatabaseTable tableDatabaseTable
  ) {
    this.tableConnection = tableConnection;
    this.tableKeyspace = tableKeyspace;
    this.tableDatabaseTable = tableDatabaseTable;
  }

  public CompletableFuture<Table> create(TableEntry entry) {
    return Table.create(tableConnection, tableKeyspace, tableDatabaseTable, entry);
  }

  public Table create(TableEntry entry, List<DatabaseColumn> columns) {
    return Table.create(tableConnection, tableKeyspace, tableDatabaseTable,
      columns, entry);
  }
}
