package net.taskwolf.table;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import lombok.RequiredArgsConstructor;
import net.taskwolf.core.database.DatabaseConfiguration;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;
import net.taskwolf.core.log.Log;
import net.taskwolf.table.structure.TableDatabaseTable;

@RequiredArgsConstructor(staticName = "create")
public class TableInjectionModule extends AbstractModule {
  @Override
  protected void configure() {

  }

  @Provides
  @Singleton
  @Named("tableConnection")
  DatabaseConnection provideTableConnection(
    DatabaseConfiguration configuration, Log log
  ) {
    var tableConnection = DatabaseConnection.create(configuration, log);
    tableConnection.connect();
    return tableConnection;
  }

  @Provides
  @Singleton
  @Named("tableKeyspace")
  DatabaseKeyspace provideTableKeyspace(
    @Named("tableConnection") DatabaseConnection connection
  ) {
    var tableKeyspace = DatabaseKeyspace.create(connection, "taskwolf_user_table",
      "SimpleStrategy", 1);
    tableKeyspace.createIfNotExists();
    tableKeyspace.use();
    return tableKeyspace;
  }

  @Provides
  @Singleton
  TableDatabaseTable provideTableDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    return TableDatabaseTable.create(connection, keyspace);
  }
}
