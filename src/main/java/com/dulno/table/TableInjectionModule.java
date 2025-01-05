package com.dulno.table;

import com.dulno.core.error.ErrorRepository;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import lombok.RequiredArgsConstructor;
import com.dulno.core.database.DatabaseConfiguration;
import com.dulno.core.database.DatabaseConnection;
import com.dulno.core.database.DatabaseKeyspace;
import com.dulno.core.log.Log;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableSizeDatabaseTable;

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
    var tableKeyspace = DatabaseKeyspace.create(connection, "dulno_user_table",
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
  TableSizeDatabaseTable provideTableSizeDatabaseTable(
    DatabaseConnection connection, DatabaseKeyspace keyspace
  ) {
    var tableSizeDatabaseTable = TableSizeDatabaseTable.create(connection,
      keyspace);
    tableSizeDatabaseTable.createIfNotExists();;
    return tableSizeDatabaseTable;
  }
}
