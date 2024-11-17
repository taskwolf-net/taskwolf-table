package com.dulno.table.structure;

import com.dulno.core.error.ErrorRepository;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.dulno.core.bundle.BundleDatabaseTable;
import com.dulno.core.database.DatabaseColumn;
import com.dulno.core.database.DatabaseConnection;
import com.dulno.core.database.DatabaseKeyspace;
import com.dulno.core.organization.OrganizationDatabaseTable;
import com.dulno.core.organization.team.TeamDatabaseTable;
import com.dulno.core.user.UserDatabaseTable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Singleton
public final class TableFactory {
  private final DatabaseConnection tableConnection;
  private final DatabaseKeyspace tableKeyspace;
  private final UserDatabaseTable userDatabaseTable;
  private final OrganizationDatabaseTable organizationDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableSizeDatabaseTable tableSizeDatabaseTable;
  private final ErrorRepository errorRepository;

  @Inject
  private TableFactory(
    @Named("tableConnection") DatabaseConnection tableConnection,
    @Named("tableKeyspace") DatabaseKeyspace tableKeyspace,
    UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, BundleDatabaseTable bundleDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, ErrorRepository errorRepository
  ) {
    this.tableConnection = tableConnection;
    this.tableKeyspace = tableKeyspace;
    this.userDatabaseTable = userDatabaseTable;
    this.organizationDatabaseTable = organizationDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableSizeDatabaseTable = tableSizeDatabaseTable;
    this.errorRepository = errorRepository;
  }

  public CompletableFuture<Table> create(TableEntry entry) {
    return Table.create(tableConnection, tableKeyspace, userDatabaseTable,
      organizationDatabaseTable, teamDatabaseTable, bundleDatabaseTable,
      tableDatabaseTable, tableSizeDatabaseTable, errorRepository, entry);
  }

  public Table create(TableEntry entry, List<DatabaseColumn> columns) {
    return Table.create(tableConnection, tableKeyspace, userDatabaseTable,
      organizationDatabaseTable, teamDatabaseTable, bundleDatabaseTable,
      tableDatabaseTable, tableSizeDatabaseTable, errorRepository, columns, entry);
  }
}
