package com.dulno.table.structure;

import com.datastax.oss.driver.api.core.cql.Row;
import com.dulno.core.error.ErrorRepository;
import com.google.common.collect.Lists;
import com.dulno.core.bundle.BundleDatabaseTable;
import com.dulno.core.database.*;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.database.paging.DatabaseDirection;
import com.dulno.core.database.paging.DatabaseOrder;
import com.dulno.core.database.paging.DatabasePage;
import com.dulno.core.organization.OrganizationDatabaseTable;
import com.dulno.core.organization.team.Team;
import com.dulno.core.organization.team.TeamDatabaseTable;
import com.dulno.core.user.UserDatabaseTable;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    TableUsageDatabaseTable tableUsageDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, ErrorRepository errorRepository,
    TableEntry entry
  ) {
    var table = new Table(connection, keyspace, entry.id(), Lists.newArrayList(),
      userDatabaseTable, organizationDatabaseTable, teamDatabaseTable,
      bundleDatabaseTable, tableDatabaseTable, tableUsageDatabaseTable,
      tableSizeDatabaseTable, errorRepository, entry);
    return table.findTableColumns().thenAccept(table::fillColumns)
      .thenApply(value -> table);
  }

  public static Table create(
    DatabaseConnection connection, DatabaseKeyspace keyspace,
    UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, BundleDatabaseTable bundleDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    TableUsageDatabaseTable tableUsageDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, ErrorRepository errorRepository,
    List<DatabaseColumn> columns, TableEntry entry
  ) {
    return new Table(connection, keyspace, entry.id(), columns,
      userDatabaseTable, organizationDatabaseTable, teamDatabaseTable,
      bundleDatabaseTable, tableDatabaseTable, tableUsageDatabaseTable,
      tableSizeDatabaseTable, errorRepository, entry);
  }

  private final UserDatabaseTable userDatabaseTable;
  private final OrganizationDatabaseTable organizationDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableUsageDatabaseTable tableUsageDatabaseTable;
  private final TableSizeDatabaseTable tableSizeDatabaseTable;
  private final ErrorRepository errorRepository;
  private final TableEntry entry;

  private Table(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> columns, UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable,
    BundleDatabaseTable bundleDatabaseTable, TableDatabaseTable tableDatabaseTable,
    TableUsageDatabaseTable tableUsageDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, ErrorRepository errorRepository,
    TableEntry entry
  ) {
    super(connection, keyspace, name, columns);
    this.userDatabaseTable = userDatabaseTable;
    this.organizationDatabaseTable = organizationDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableUsageDatabaseTable = tableUsageDatabaseTable;
    this.tableSizeDatabaseTable = tableSizeDatabaseTable;
    this.errorRepository = errorRepository;
    this.entry = entry;
  }

  public CompletableFuture<Boolean> insertContent(TableRow row) {
    var sizeAddition = row.size();
    return findTableBundleOwner().thenCompose(bundleOwner ->
      checkDatabaseSizeLimit(bundleOwner, sizeAddition).thenApply(limitReached ->
        insertContent(row, bundleOwner, sizeAddition, limitReached)));
  }

  private boolean insertContent(
    TableRow row, UUID bundleOwner, long sizeAddition, boolean limitReached
  ) {
    if (limitReached) {
      return false;
    }
    updateTableSize(sizeAddition, bundleOwner);
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
    return findTableBundleOwner().thenCompose(bundleOwner -> findContent(id)
      .thenCompose(previousRow -> updateContent(id, previousRow, row, bundleOwner)));
  }

  public CompletableFuture<Boolean> updateContent(
    UUID id, DatabaseRow previousRow, TableRow newRow, UUID bundleOwner
    ) {
    var previusTableRow = TableRow.of(errorRepository, previousRow, columns());
    var sizeAddition = newRow.size() - previusTableRow.size();
    return checkDatabaseSizeLimit(bundleOwner, sizeAddition)
      .thenApply(limitReached -> updateContent(id, previousRow, newRow,
        bundleOwner, sizeAddition, limitReached));
  }

  private boolean updateContent(
    UUID id, DatabaseRow previousRow, TableRow newRow, UUID bundleOwner,
    long sizeAddition, boolean limitReached
  ) {
    if (limitReached) {
      return false;
    }
    updateTableSize(sizeAddition, bundleOwner);
    update(DatabaseCondition.of("owner", entry.owner(), "timestamp",
        previousRow.findCell(1).longValue(), "id", id),
      DatabaseRow.of(createRowValues(newRow)));
    return true;
  }

  private CompletableFuture<Boolean> checkDatabaseSizeLimit(
    UUID bundleOwner, long sizeAddition
  ) {
    return bundleDatabaseTable.findBundle(bundleOwner)
      .thenCompose(bundle -> tableUsageDatabaseTable.findUsage(bundleOwner)
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

  private static final int MAX_PAGE_SIZE = 100;

  public CompletableFuture<DatabasePage<TableRow>> firstContentPage(
    int pageSize
  ) {
    if (pageSize < 0) {
      pageSize = MAX_PAGE_SIZE;
    }
    return selectPage(entry.owner(), DatabaseCondition.empty(),
      DatabaseOrder.DESCENDING, Math.min(pageSize, MAX_PAGE_SIZE), 0)
      .thenApply(this::createContentPage);
  }

  public CompletableFuture<DatabasePage<TableRow>> nextContentPage(
    String pageState, int pageSize
  ) {
    if (pageSize < 0) {
      pageSize = MAX_PAGE_SIZE;
    }
    return shiftPage(entry.owner(), DatabaseCondition.empty(),
      DatabaseOrder.DESCENDING, Math.min(pageSize, MAX_PAGE_SIZE), pageState,
      DatabaseDirection.FORWARD, DatabaseDirection.FORWARD)
      .thenApply(this::createContentPage);
  }

  private DatabasePage<TableRow> createContentPage(
    DatabasePage<DatabaseRow> page
  ) {
    return DatabasePage.create(
      page.content().stream().map(row ->
        TableRow.of(errorRepository, row, columns())).toList(),
      page.pageState(), page.pageNumber());
  }

  public CompletableFuture<DatabaseRow> findContent(UUID id) {
    return selectRow(DatabaseCondition.of("owner", entry.owner(), "id", id));
  }

  public CompletableFuture<Void> removeContent(UUID id) {
    return findTableBundleOwner().thenCompose(bundleOwner ->
      removeContent(id, bundleOwner));
  }

  public CompletableFuture<Void> removeContent(UUID id, UUID bundleOwner) {
    return findContent(id).thenCompose(row -> removeContent(id, row, bundleOwner));
  }

  private CompletableFuture<Void> removeContent(
    UUID id, DatabaseRow row, UUID bundleOwner
  ) {
    var sizeAddition = - TableRow.of(errorRepository, row, columns()).size();
    return updateTableSize(sizeAddition, bundleOwner)
      .thenCompose(value -> delete(DatabaseCondition.of("owner", entry.owner(),
        "timestamp", row.findCell(1).longValue(), "id", id)));
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
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("owner", DatabaseDataType.UUID,
      DatabaseColumn.Type.PARTITION_KEY));
    columns.add(DatabaseColumn.create("timestamp", DatabaseDataType.BIGINT,
      DatabaseColumn.Type.CLUSTERING_KEY));
    columns.add(DatabaseColumn.create("id", DatabaseDataType.UUID,
      DatabaseColumn.Type.CLUSTERING_KEY));
    for (var row : rows) {
      var column = createDatabaseColumnEntry(row);
      if (column.name().equalsIgnoreCase("id") ||
        column.name().equalsIgnoreCase("timestamp") ||
        column.name().equalsIgnoreCase("owner")
      ) {
        continue;
      }
      columns.add(column);
    }
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

  public CompletableFuture<File> download() {
    try {
      var file = new File(System.getProperty("user.dir") + "/table/" +
        entry.id() + ".csv");
      file.getParentFile().mkdirs();
      file.createNewFile();
      var fileWriter = new FileWriter(file);
      var csvWriter = new CSVWriter(fileWriter);
      csvWriter.writeNext(columns().stream().map(DatabaseColumn::name)
        .filter(column -> !column.equals("owner")).toArray(String[]::new));
      var futureResponse = new CompletableFuture<Void>();
      firstContentPage(MAX_PAGE_SIZE).thenAccept(page ->
        appendCSVRows(page, csvWriter, futureResponse));
      return futureResponse.thenApply(value -> file);
    } catch (Exception exception) {
      errorRepository.processError(exception);
      return CompletableFuture.completedFuture(null);
    }
  }

  private void appendCSVRows(
    DatabasePage<TableRow> page, CSVWriter csvWriter,
    CompletableFuture<Void> futureResponse
  ) {
    try {
      writeCSVPage(page, csvWriter);
      if (page.pageState().isEmpty()) {
        csvWriter.close();
        futureResponse.complete(null);
        return;
      }
      nextContentPage(page.pageState(), MAX_PAGE_SIZE)
        .thenAccept(nextPage -> appendCSVRows(nextPage, csvWriter, futureResponse));
    } catch (Exception exception) {
      errorRepository.processError(exception);
      futureResponse.complete(null);
    }
  }

  private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private void writeCSVPage(DatabasePage<TableRow> page, CSVWriter csvWriter) {
    for (var row : page.content()) {
      var cells = row.cells();
      var data = new String[cells.size() - 1];
      var dataIndex = 0;
      for (var cell : cells) {
        if (cell.column().equals("owner")) {
          continue;
        }
        var value = cell.value();
        if (value == null) {
          data[dataIndex] = "";
        } else if (cell.column().equals("timestamp")) {
          data[dataIndex] = TIMESTAMP_FORMAT.format(new Date((long) cell.value()));
        } else {
          data[dataIndex] = value.toString();
        }
        dataIndex++;
      }
      csvWriter.writeNext(data);
    }
  }

  @Override
  public CompletableFuture<Void> truncate() {
    findTableBundleOwner()
      .thenCompose(bundleOwner -> tableSizeDatabaseTable.findSize(entry.id())
        .thenCompose(size -> updateTableSize(-size, bundleOwner)));
    return super.truncate();
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
    firstContentPage(MAX_PAGE_SIZE)
      .thenAccept(page -> recalculateTableSize(page, 0L));
  }

  private void recalculateTableSize(DatabasePage<TableRow> page, long size) {
    var newSize = size + page.content().stream().mapToLong(TableRow::size).sum();
    if (page.pageState().isEmpty()) {
      tableSizeDatabaseTable.findSize(entry.id())
        .thenApply(oldSize -> newSize - oldSize)
        .thenAccept(sizeAddition -> findTableBundleOwner()
          .thenAccept(bundleOwner -> updateTableSize(sizeAddition, bundleOwner)));
      return;
    }
    nextContentPage(page.pageState(), MAX_PAGE_SIZE)
      .thenAccept(nextPage -> recalculateTableSize(page, newSize));
  }

  @Override
  public CompletableFuture<Void> drop(String addition) {
    findTableBundleOwner()
      .thenCompose(bundleOwner -> tableSizeDatabaseTable.findSize(entry.id())
        .thenCompose(size -> tableUsageDatabaseTable.updateUsage(bundleOwner, -size)
          .thenCompose(value -> tableSizeDatabaseTable.deleteSize(entry.id()))));
    return super.drop(addition);
  }

  private CompletableFuture<Void> updateTableSize(
    long sizeAddition, UUID bundleOwner
  ) {
    return tableUsageDatabaseTable.updateUsage(bundleOwner, sizeAddition)
      .thenCompose(usageValue -> tableSizeDatabaseTable.updateSize(entry.id(),
          sizeAddition)
        .thenCompose(sizeValue -> tableSizeDatabaseTable.findSize(entry.id())
          .thenCompose(size -> tableDatabaseTable.updateTableSize(entry.id(),
            size))));
  }

  public CompletableFuture<UUID> findTableBundleOwner() {
    var owner = entry.owner();
    return userDatabaseTable.userExists(owner)
      .thenCompose(userExists -> organizationDatabaseTable.organizationExists(owner)
        .thenCompose(organizationExists -> userExists || organizationExists ?
          CompletableFuture.completedFuture(owner) :
          teamDatabaseTable.findTeam(owner).thenApply(Team::organizationId)));
  }
}
