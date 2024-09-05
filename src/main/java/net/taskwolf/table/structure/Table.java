package net.taskwolf.table.structure;

import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.Lists;
import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.database.*;
import net.taskwolf.core.database.condition.DatabaseCondition;
import net.taskwolf.core.database.paging.DatabaseDirection;
import net.taskwolf.core.database.paging.DatabaseOrder;
import net.taskwolf.core.database.paging.DatabasePage;
import net.taskwolf.core.organization.OrganizationDatabaseTable;
import net.taskwolf.core.organization.team.Team;
import net.taskwolf.core.organization.team.TeamDatabaseTable;
import net.taskwolf.core.user.UserDatabaseTable;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

public final class Table extends DatabaseTable {
  public static CompletableFuture<Table> create(
    DatabaseConnection connection, DatabaseKeyspace keyspace,
    UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, BundleDatabaseTable bundleDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, TableEntry entry
  ) {
    var table = new Table(connection, keyspace, entry.id(), Lists.newArrayList(),
      userDatabaseTable, organizationDatabaseTable, teamDatabaseTable,
      bundleDatabaseTable, tableDatabaseTable, tableSizeDatabaseTable, entry);
    return table.findTableColumns().thenAccept(table::fillColumns)
      .thenApply(value -> table);
  }

  public static Table create(
    DatabaseConnection connection, DatabaseKeyspace keyspace,
    UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, BundleDatabaseTable bundleDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, List<DatabaseColumn> columns,
    TableEntry entry
  ) {
    return new Table(connection, keyspace, entry.id(), columns,
      userDatabaseTable, organizationDatabaseTable, teamDatabaseTable,
      bundleDatabaseTable, tableDatabaseTable, tableSizeDatabaseTable, entry);
  }

  private final UserDatabaseTable userDatabaseTable;
  private final OrganizationDatabaseTable organizationDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableSizeDatabaseTable tableSizeDatabaseTable;
  private final TableEntry entry;

  private Table(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns, UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable,
    BundleDatabaseTable bundleDatabaseTable, TableDatabaseTable tableDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, TableEntry entry
  ) {
    super(connection, keyspace, name, columns);
    this.userDatabaseTable = userDatabaseTable;
    this.organizationDatabaseTable = organizationDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableSizeDatabaseTable = tableSizeDatabaseTable;
    this.entry = entry;
  }

  public CompletableFuture<Boolean> insertContent(TableRow row) {
    var sizeAddition = row.size();
    var totalSize = entry.size() + sizeAddition;
    return findTableBundleOwner().thenCompose(bundleOwner ->
      checkDatabaseSizeLimit(bundleOwner, sizeAddition).thenApply(limitReached ->
        insertContent(row, bundleOwner, sizeAddition, totalSize, limitReached)));
  }

  private boolean insertContent(
    TableRow row, UUID bundleOwner, long sizeAddition, long totalSize,
    boolean limitReached
  ) {
    if (limitReached) {
      return false;
    }
    tableSizeDatabaseTable.updateSize(bundleOwner, sizeAddition);
    tableDatabaseTable.updateTableSize(entry, totalSize);
    insert(DatabaseRow.of(createRowValues(row)));
    return true;
  }

  public CompletableFuture<UUID> generateAvailableContentId() {
    var futureResponse = new CompletableFuture<UUID>();
    var id = UUID.randomUUID();
    contentExists(id).thenApply(exists -> exists ?
      generateAvailableContentId().thenApply(futureResponse::complete) :
      CompletableFuture.completedFuture(futureResponse.complete(id)));
    return futureResponse;
  }

  public CompletableFuture<Boolean> updateContent(UUID id, TableRow row) {
    return findTableBundleOwner().thenCompose(bundleOwner ->
      selectRow(DatabaseCondition.of("owner", entry.owner(), "id", id))
        .thenApply(previousRow -> TableRow.of(previousRow, columns()))
        .thenApply(previousRow -> row.size() - previousRow.size())
        .thenCompose(sizeAddition -> checkDatabaseSizeLimit(bundleOwner, sizeAddition)
          .thenApply(limitReached -> updateContent(id, row, bundleOwner,
            sizeAddition, entry.size() + sizeAddition, limitReached))));
  }

  private boolean updateContent(
    UUID id, TableRow row, UUID bundleOwner, long sizeAddition, long totalSize,
    boolean limitReached
  ) {
    if (limitReached) {
      return false;
    }
    tableSizeDatabaseTable.updateSize(bundleOwner, sizeAddition);
    tableDatabaseTable.updateTableSize(entry, totalSize);
    update(DatabaseCondition.of("owner", entry.owner(), "id", id),
      DatabaseRow.of(createRowValues(row)));
    return true;
  }

  private CompletableFuture<Boolean> checkDatabaseSizeLimit(
    UUID bundleOwner, long sizeAddition
  ) {
    return bundleDatabaseTable.findBundle(bundleOwner)
      .thenCompose(bundle -> tableSizeDatabaseTable.findSize(bundleOwner)
        .thenApply(size -> size + sizeAddition)
        .thenApply(dataSize -> bundle.databaseDataLimit() > 0 &&
          dataSize * Math.pow(10, -9) >= bundle.databaseDataLimit()));
  }

  private Object[] createRowValues(TableRow row) {
    var cells = row.cells();
    var columns = columns();
    var values = new Object[columns.size()];
    for (var i = 0; i < columns.size(); i++) {
      var column = columns.get(i);
      values[i] = findRowValueForColumn(column, cells);
    }
    return values;
  }

  private Object findRowValueForColumn(DatabaseColumn column, List<TableCell> cells) {
    for (var cell : cells) {
      if (column.name().equalsIgnoreCase(cell.column())) {
        return cell.value();
      }
    }
    return "";
  }

  public CompletableFuture<Boolean> contentExists(UUID id) {
    return exists(DatabaseCondition.of("owner", entry.owner(), "id", id));
  }

  private static final int PAGE_SIZE = 5;

  public CompletableFuture<DatabasePage<TableRow>> findContentPage(int targetPage) {
    return selectPage(entry.owner(), DatabaseCondition.empty(),
      DatabaseOrder.ASCENDING, PAGE_SIZE, targetPage)
      .thenApply(this::createContentPage);
  }

  public CompletableFuture<DatabasePage<TableRow>> shiftContentPage(
    String pageState, DatabaseDirection startingPoint, DatabaseDirection direction
  ) {
    return shiftPage(entry.owner(), DatabaseCondition.empty(),
      DatabaseOrder.ASCENDING, PAGE_SIZE, pageState, startingPoint, direction)
      .thenApply(this::createContentPage);
  }

  private DatabasePage<TableRow> createContentPage(
    DatabasePage<DatabaseRow> page
  ) {
    return DatabasePage.create(
      page.content().stream().map(row -> TableRow.of(row, columns())).toList(),
      page.pageState(), page.pageNumber());
  }

  public void removeContent(UUID id) {
    findTableBundleOwner().thenCompose(bundleOwner ->
      selectRow(DatabaseCondition.of("owner", entry.owner(), "id", id))
        .thenApply(row -> TableRow.of(row, columns()))
        .thenApply(row -> - row.size())
        .thenCompose(sizeAddition ->
          tableSizeDatabaseTable.updateSize(bundleOwner, sizeAddition)
            .thenAccept(value -> tableDatabaseTable.updateTableSize(entry,
              entry.size() + sizeAddition))
            .thenAccept(value -> delete(DatabaseCondition.of("owner",
              entry.owner(), "id", id)))));
  }

  private CompletableFuture<List<DatabaseColumn>> findTableColumns() {
    var query = new StringBuilder("SELECT * FROM system_schema.columns WHERE ");
    query.append("keyspace_name = '");
    query.append(keyspace().name());
    query.append("' AND table_name = '");
    query.append(name());
    query.append("';");
    return connection().execute(query)
      .thenApply(result -> createDatabaseColumns(result.currentPage()));
  }

  private List<DatabaseColumn> createDatabaseColumns(Iterable<Row> rows) {
    DatabaseColumn partitionKeyColumn = null;
    DatabaseColumn clusteringKeyColumn = null;
    var columns = Lists.<DatabaseColumn>newArrayList();
    for (var row : rows) {
      var column = createDatabaseColumnEntry(row);
      if (column.type().isPartitionKey()) {
        partitionKeyColumn = column;
      } else if (column.type().isClusteringKey()) {
        clusteringKeyColumn = column;
      } else {
        columns.add(column);
      }
    }
    columns.add(0, clusteringKeyColumn);
    columns.add(0, partitionKeyColumn);
    return columns;
  }

  private DatabaseColumn createDatabaseColumnEntry(Row row) {
    var columnName = row.getString("column_name");
    var kind = row.getString("kind");
    var columnType = switch (kind) {
      case "partition_key" -> DatabaseColumn.Type.PARTITION_KEY;
      case "clustering" -> DatabaseColumn.Type.CLUSTERING_KEY;
      default -> DatabaseColumn.Type.REGULAR;
    };
    var dataType = row.getString("type").toUpperCase();
    if (dataType.contains("LIST")) {
      return DatabaseListColumn.create(columnName, DatabaseDataType.valueOf(
        dataType.replace("LIST", "").replace("<", "").replace(">", "")),
        columnType);
    }
    return DatabaseColumn.create(columnName, DatabaseDataType.valueOf(dataType),
      columnType);
  }

  private static final Pattern COLUMN_PATTERN =
    Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

  @Override
  public CompletableFuture<Void> addColumn(DatabaseColumn column) {
    if (!COLUMN_PATTERN.matcher(column.name()).matches()) {
      return CompletableFuture.completedFuture(null);
    }
    return super.addColumn(column).thenAccept(value -> recalculateTableSize());
  }

  @Override
  public CompletableFuture<Void> dropColumn(String columnName) {
    if (!COLUMN_PATTERN.matcher(columnName).matches()) {
      return CompletableFuture.completedFuture(null);
    }
    return super.dropColumn(columnName).thenAccept(value -> recalculateTableSize());
  }

  private void recalculateTableSize() {
    var futureSize = selectAllRows().thenApply(rows -> rows.stream()
      .mapToLong(row -> TableRow.of(row, columns()).size()).sum());
    futureSize.thenAccept(size ->
      findTableBundleOwner().thenCompose(bundleOwner ->
        tableSizeDatabaseTable.updateSize(bundleOwner, size - entry.size())
          .thenAccept(value -> tableDatabaseTable.updateTableSize(entry, size))));
  }

  @Override
  public void drop(String addition) {
    super.drop(addition);
    findTableBundleOwner().thenCompose(bundleOwner ->
      tableSizeDatabaseTable.updateSize(bundleOwner, -entry.size()));
  }

  private CompletableFuture<UUID> findTableBundleOwner() {
    var owner = entry.owner();
    return userDatabaseTable.userExists(owner)
      .thenCompose(userExists -> organizationDatabaseTable.organizationExists(owner)
        .thenCompose(organizationExists -> userExists || organizationExists ?
          CompletableFuture.completedFuture(owner) :
          teamDatabaseTable.findTeam(owner).thenApply(Team::organizationId)));
  }
}
