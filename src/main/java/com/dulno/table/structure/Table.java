package com.dulno.table.structure;

import com.dulno.core.error.ErrorRepository;
import com.dulno.core.iterator.AsyncIterator;
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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public final class Table extends DatabaseTable {
  public static Table create(
    DatabaseConnection connection, DatabaseKeyspace keyspace,
    UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, BundleDatabaseTable bundleDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    TableColumnDatabaseTable tableColumnDatabaseTable,
    TableUsageDatabaseTable tableUsageDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, ErrorRepository errorRepository,
    TableEntry entry, List<TableColumn> tableColumns
  ) {
    var columns = Lists.<DatabaseColumn>newArrayList();
    columns.add(DatabaseColumn.create("owner", DatabaseDataType.UUID,
      DatabaseColumn.Type.PARTITION_KEY));
    columns.add(DatabaseColumn.create("timestamp", DatabaseDataType.BIGINT,
      DatabaseColumn.Type.CLUSTERING_KEY));
    columns.add(DatabaseColumn.create("id", DatabaseDataType.UUID,
      DatabaseColumn.Type.CLUSTERING_KEY));
    for (var column : tableColumns) {
      columns.add(DatabaseColumn.create(column.id(), column.type().dataType()));
    }
    return new Table(connection, keyspace, entry.id(), columns,
      userDatabaseTable, organizationDatabaseTable, teamDatabaseTable,
      bundleDatabaseTable, tableDatabaseTable, tableColumnDatabaseTable,
      tableUsageDatabaseTable, tableSizeDatabaseTable, errorRepository, entry,
      Lists.newArrayList(tableColumns));
  }

  private final UserDatabaseTable userDatabaseTable;
  private final OrganizationDatabaseTable organizationDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
  private final TableUsageDatabaseTable tableUsageDatabaseTable;
  private final TableSizeDatabaseTable tableSizeDatabaseTable;
  private final ErrorRepository errorRepository;
  private final TableEntry entry;
  private final List<TableColumn> tableColumns;

  private Table(
    DatabaseConnection connection, DatabaseKeyspace keyspace, String name,
    List<DatabaseColumn> databaseColumns, UserDatabaseTable userDatabaseTable,
    OrganizationDatabaseTable organizationDatabaseTable,
    TeamDatabaseTable teamDatabaseTable,
    BundleDatabaseTable bundleDatabaseTable, TableDatabaseTable tableDatabaseTable,
    TableColumnDatabaseTable tableColumnDatabaseTable,
    TableUsageDatabaseTable tableUsageDatabaseTable,
    TableSizeDatabaseTable tableSizeDatabaseTable, ErrorRepository errorRepository,
    TableEntry entry, List<TableColumn> tableColumns
  ) {
    super(connection, keyspace, name, databaseColumns);
    this.userDatabaseTable = userDatabaseTable;
    this.organizationDatabaseTable = organizationDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableColumnDatabaseTable = tableColumnDatabaseTable;
    this.tableUsageDatabaseTable = tableUsageDatabaseTable;
    this.tableSizeDatabaseTable = tableSizeDatabaseTable;
    this.errorRepository = errorRepository;
    this.entry = entry;
    this.tableColumns = tableColumns;
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

  public CompletableFuture<File> download() {
    try {
      var file = new File(System.getProperty("user.dir") + "/table/" +
        entry.id() + ".csv");
      file.getParentFile().mkdirs();
      file.createNewFile();
      var fileWriter = new FileWriter(file);
      var csvWriter = new CSVWriter(fileWriter);
      csvWriter.writeNext(createCSVHeader());
      var futureResponse = new CompletableFuture<Void>();
      firstContentPage(MAX_PAGE_SIZE).thenAccept(page ->
        appendCSVRows(page, csvWriter, "", futureResponse));
      return futureResponse.thenApply(value -> file);
    } catch (Exception exception) {
      errorRepository.processError(exception);
      return CompletableFuture.completedFuture(null);
    }
  }

  private String[] createCSVHeader() {
    var header = Lists.<String>newArrayList();
    header.add("dulno_timestamp");
    header.add("dulno_id");
    for (var column : tableColumns) {
      header.add(column.name() + " (" + column.type() + ")");
    }
    return header.toArray(String[]::new);
  }

  private void appendCSVRows(
    DatabasePage<TableRow> page, CSVWriter csvWriter, String previousPageState,
    CompletableFuture<Void> futureResponse
  ) {
    try {
      if (!previousPageState.equals(page.pageState())) {
        writeCSVPage(page, csvWriter);
      }
      if (page.pageState().isEmpty() || previousPageState.equals(page.pageState())) {
        csvWriter.close();
        futureResponse.complete(null);
        return;
      }
      nextContentPage(page.pageState(), MAX_PAGE_SIZE).thenAccept(nextPage ->
        appendCSVRows(nextPage, csvWriter, page.pageState(), futureResponse));
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
    return super.truncate()
      .thenCompose(value -> findTableBundleOwner()
        .thenCompose(bundleOwner -> tableSizeDatabaseTable.findSize(entry.id())
          .thenCompose(size -> updateTableSize(-size, bundleOwner))));
  }

  public CompletableFuture<Void> addColumn(String name, TableColumnType type) {
    return tableColumnDatabaseTable.generateAvailableColumnId()
      .thenApply(id -> TableColumn.create(id, entry.id(), type, name))
      .thenCompose(column -> tableColumnDatabaseTable.insertColumn(column)
        .thenAccept(value -> tableColumns.add(column))
        .thenCompose(value -> tableDatabaseTable.updateTableColumns(entry.id(),
          tableColumns.stream().map(TableColumn::id).toList()))
        .thenCompose(value -> super.addColumn(column.toDatabaseColumn()))
        .thenCompose(value -> createIndexAsyncIfNotExists(column.id()))
        .thenCompose(value -> recalculateTableSize()));
  }

  public CompletableFuture<Void> dropColumn(String columnId) {
    var columnOptional = tableColumns.stream()
      .filter(entry -> entry.id().equals(columnId)).findFirst();
    if (columnOptional.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    var column = columnOptional.get();
    tableColumns.remove(column);
    return tableColumnDatabaseTable.deleteColumn(column.id())
      .thenCompose(value -> dropIndexAsyncIfExists(column.id()))
      .thenCompose(value -> super.dropColumn(column.id()))
      .thenCompose(value -> tableDatabaseTable.updateTableColumns(entry.id(),
        tableColumns.stream().map(TableColumn::id).toList()))
      .thenCompose(value -> recalculateTableSize());
  }

  private CompletableFuture<Void> recalculateTableSize() {
    var futureResponse = new CompletableFuture<Void>();
    firstContentPage(MAX_PAGE_SIZE)
      .thenAccept(page -> recalculateTableSize(page, 0L, "", futureResponse));
    return futureResponse;
  }

  private void recalculateTableSize(
    DatabasePage<TableRow> page, long size, String previousPageState,
    CompletableFuture<Void> futureResponse
  ) {
    var newSize = size + ((!previousPageState.equals(page.pageState())) ?
      page.content().stream().mapToLong(TableRow::size).sum() : 0);
    if (page.pageState().isEmpty() || previousPageState.equals(page.pageState())) {
      tableSizeDatabaseTable.findSize(entry.id())
        .thenApply(oldSize -> newSize - oldSize)
        .thenAccept(sizeAddition -> findTableBundleOwner()
          .thenAccept(bundleOwner -> updateTableSize(sizeAddition, bundleOwner)
            .thenAccept(value -> futureResponse.complete(null))));
      return;
    }
    nextContentPage(page.pageState(), MAX_PAGE_SIZE).thenAccept(nextPage ->
      recalculateTableSize(page, newSize, page.pageState(), futureResponse));
  }

  @Override
  public CompletableFuture<Void> drop(String addition) {
    return super.drop(addition)
      .thenCompose(dropValue -> findTableBundleOwner()
        .thenCompose(bundleOwner -> tableSizeDatabaseTable.findSize(entry.id())
          .thenCompose(size -> tableUsageDatabaseTable.updateUsage(bundleOwner, -size)
            .thenCompose(value -> tableSizeDatabaseTable.deleteSize(entry.id()))))
        .thenCompose(value -> AsyncIterator.execute(tableColumns,
          column -> tableColumnDatabaseTable.deleteColumn(column.id())))
        .thenApply(value -> null));
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

  public List<TableColumn> tableColumns() {
    return List.copyOf(tableColumns);
  }
}
