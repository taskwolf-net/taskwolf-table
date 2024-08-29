package net.taskwolf.table.structure;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;
import net.taskwolf.core.organization.OrganizationDatabaseTable;
import net.taskwolf.core.organization.team.TeamDatabaseTable;
import net.taskwolf.core.user.UserDatabaseTable;

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

  @Inject
  private TableFactory(
    @Named("tableConnection") DatabaseConnection tableConnection,
    @Named("tableKeyspace") DatabaseKeyspace tableKeyspace,
    UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, BundleDatabaseTable bundleDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable
  ) {
    this.tableConnection = tableConnection;
    this.tableKeyspace = tableKeyspace;
    this.userDatabaseTable = userDatabaseTable;
    this.organizationDatabaseTable = organizationDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableSizeDatabaseTable = tableSizeDatabaseTable;
  }

  public CompletableFuture<Table> create(TableEntry entry) {
    return Table.create(tableConnection, tableKeyspace, userDatabaseTable,
      organizationDatabaseTable, teamDatabaseTable, bundleDatabaseTable,
      tableDatabaseTable, tableSizeDatabaseTable, entry);
  }

  public Table create(TableEntry entry, List<DatabaseColumn> columns) {
    return Table.create(tableConnection, tableKeyspace, userDatabaseTable,
      organizationDatabaseTable, teamDatabaseTable, bundleDatabaseTable,
      tableDatabaseTable, tableSizeDatabaseTable, columns, entry);
  }
}
