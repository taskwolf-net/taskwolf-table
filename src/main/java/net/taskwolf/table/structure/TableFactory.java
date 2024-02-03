package net.taskwolf.table.structure;

import com.google.inject.Singleton;
import com.google.inject.name.Named;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Singleton
public final class TableFactory {
  private final DatabaseConnection tableConnection;
  private final DatabaseKeyspace tableKeyspace;

  private TableFactory(
    @Named("tableConnection") DatabaseConnection tableConnection,
    @Named("tableKeyspace") DatabaseKeyspace tableKeyspace
  ) {
    this.tableConnection = tableConnection;
    this.tableKeyspace = tableKeyspace;
  }

  public CompletableFuture<Table> create(UUID id) {
    return Table.create(tableConnection, tableKeyspace, id);
  }
}
