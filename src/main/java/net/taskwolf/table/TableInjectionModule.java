package net.taskwolf.table;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;

@RequiredArgsConstructor(staticName = "create")
public class TableInjectionModule extends AbstractModule {
  @Override
  protected void configure() {

  }

  @Provides
  @Singleton
  TableDatabaseTable provideTableDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var tableDatabaseTable = TableDatabaseTable.create(connection, keyspace);
    tableDatabaseTable.createIfNotExists();
    return tableDatabaseTable;
  }
}
