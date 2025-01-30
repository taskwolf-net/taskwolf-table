package com.dulno.table.structure;

import com.dulno.core.error.ErrorRepository;
import com.google.common.collect.Lists;
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

import java.util.Comparator;
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
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
  private final TableUsageDatabaseTable tableUsageDatabaseTable;
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
    TableColumnDatabaseTable tableColumnDatabaseTable,
    TableUsageDatabaseTable tableUsageDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, ErrorRepository errorRepository
  ) {
    this.tableConnection = tableConnection;
    this.tableKeyspace = tableKeyspace;
    this.userDatabaseTable = userDatabaseTable;
    this.organizationDatabaseTable = organizationDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableColumnDatabaseTable = tableColumnDatabaseTable;
    this.tableUsageDatabaseTable = tableUsageDatabaseTable;
    this.tableSizeDatabaseTable = tableSizeDatabaseTable;
    this.errorRepository = errorRepository;
  }

  public CompletableFuture<Table> create(TableEntry entry) {
    return tableColumnDatabaseTable.findTableColumns(entry.id())
      .thenApply(columns -> columns.stream()
        .sorted(Comparator.comparing(column -> entry.columns().indexOf(column.id())))
        .toList())
      .thenApply(columns -> Table.create(tableConnection, tableKeyspace,
        userDatabaseTable, organizationDatabaseTable, teamDatabaseTable,
        bundleDatabaseTable, tableDatabaseTable, tableColumnDatabaseTable,
        tableUsageDatabaseTable, tableSizeDatabaseTable, errorRepository,
        entry, columns));
  }

  public Table create(TableEntry entry, List<TableColumn> tableColumns) {
    return Table.create(tableConnection, tableKeyspace, userDatabaseTable,
      organizationDatabaseTable, teamDatabaseTable, bundleDatabaseTable,
      tableDatabaseTable, tableColumnDatabaseTable, tableUsageDatabaseTable,
      tableSizeDatabaseTable, errorRepository, entry, tableColumns);
  }
}
