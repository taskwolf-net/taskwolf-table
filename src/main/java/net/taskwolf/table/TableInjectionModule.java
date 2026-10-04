package net.taskwolf.table;

import net.taskwolf.core.database.DatabaseConfiguration;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;
import net.taskwolf.core.error.ErrorRepository;
import net.taskwolf.core.log.Log;
import net.taskwolf.table.structure.TableColumnDatabaseTable;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableSizeDatabaseTable;
import net.taskwolf.table.structure.TableUsageDatabaseTable;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(staticName = "create")
public class TableInjectionModule extends AbstractModule {
  @Override
  protected void configure() {

  }

  @Provides
  @Singleton
  @Named("tableConnection")
  DatabaseConnection provideTableConnection(
    DatabaseConfiguration configuration, Log log, ErrorRepository errorRepository
  ) {
    var tableConnection = DatabaseConnection.create(configuration, log);
    tableConnection.connect();
    tableConnection.errorRepository(errorRepository);
    return tableConnection;
  }

  @Provides
  @Singleton
  @Named("tableKeyspace")
  DatabaseKeyspace provideTableKeyspace(
    @Named("tableConnection") DatabaseConnection connection
  ) {
    var tableKeyspace = DatabaseKeyspace.create(connection, "taskwolf_user_table",
      "SimpleStrategy", 2);
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

  @Provides
  @Singleton
  TableColumnDatabaseTable provideColumnTableDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    return TableColumnDatabaseTable.create(connection, keyspace);
  }

  @Provides
  @Singleton
  TableUsageDatabaseTable provideTableUsageDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var tableUsageDatabaseTable = TableUsageDatabaseTable.create(connection,
      keyspace);
    tableUsageDatabaseTable.createIfNotExists();;
    return tableUsageDatabaseTable;
  }

  @Provides
  @Singleton
  TableSizeDatabaseTable provideTableSizeDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var tableSizeDatabaseTable = TableSizeDatabaseTable.create(connection,
      keyspace);
    tableSizeDatabaseTable.createIfNotExists();;
    return tableSizeDatabaseTable;
  }
}
