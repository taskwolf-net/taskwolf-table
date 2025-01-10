package com.dulno.table.access;

import com.dulno.core.database.DatabaseRow;
import com.dulno.core.error.ErrorRepository;
import com.dulno.table.structure.*;
import com.dulno.workflow.WorkflowModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import com.dulno.core.access.DulnoRequestBody;
import com.dulno.core.bundle.BundleDatabaseTable;
import com.dulno.core.database.DatabaseColumn;
import com.dulno.core.database.DatabaseDataType;
import com.dulno.core.database.DatabaseTable;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.iterator.AsyncIterator;
import com.dulno.core.organization.team.Team;
import com.dulno.core.organization.team.TeamDatabaseTable;
import com.dulno.core.organization.team.TeamTargetDatabaseTable;
import com.dulno.core.user.User;
import com.dulno.core.user.UserDatabaseTable;
import com.dulno.core.user.UserTargetDatabaseTable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@RestController
public final class TableModificationController extends TableController {
  private final TableFactory tableFactory;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final WorkflowModule workflowModule;
  private final ErrorRepository errorRepository;

  private TableModificationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable,
    TableFactory tableFactory, BundleDatabaseTable bundleDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, WorkflowModule workflowModule,
    ErrorRepository errorRepository
  ) {
    super(secretKey, userDatabaseTable, tableDatabaseTable,
      userTargetDatabaseTable, teamTargetDatabaseTable);
    this.tableFactory = tableFactory;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.workflowModule = workflowModule;
    this.errorRepository = errorRepository;
  }

  @RequestMapping(path = "/table/create/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> createTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var name = body.getString("name", 64);
    if (name.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return findUser(request).thenCompose(user ->
      userTargetDatabaseTable().findTargetSecured(user.id()).thenCompose(target ->
        findTableOwner(user, target).thenCompose(owner ->
          tableDatabaseTable().generateAvailableTableId().thenCompose(tableId ->
            checkDatabaseNumberLimit(user, target).thenCompose(limitReached ->
              createTable(tableId, owner, user.id(), name,
                limitReached, response))))));
  }

  private CompletableFuture<UUID> findTableOwner(User user, UUID target) {
    return user.id().equals(target) ?
      CompletableFuture.completedFuture(target) :
      teamTargetDatabaseTable().findTargetSecured(user.id())
        .thenApply(team -> team.orElse(target));
  }

  private CompletableFuture<Boolean> checkDatabaseNumberLimit(
    User user, UUID target
  ) {
    return findOwnersOfTarget(user, target)
      .thenCompose(owners -> AsyncIterator.execute(owners, owner ->
          tableDatabaseTable().findTableCount(owner))
        .thenApply(sizes -> sizes.stream().mapToLong(Long::longValue).sum())
        .thenCompose(number -> bundleDatabaseTable.findBundle(target)
          .thenApply(bundle -> bundle.databaseNumberLimit() > 0 &&
            number >= bundle.databaseNumberLimit())));
  }

  private CompletableFuture<List<UUID>> findOwnersOfTarget(User user, UUID target) {
    return user.id().equals(target) ?
      CompletableFuture.completedFuture(Lists.newArrayList(target)) :
      teamDatabaseTable.findTeamsByOrganization(target).thenApply(teams ->
        Stream.concat(teams.stream().map(Team::id).toList().stream(),
          Stream.of(target)).toList());
  }

  private CompletableFuture<Map<String, Object>> createTable(
    String tableId, UUID owner, UUID creator, String name, boolean limitReached,
    HttpServletResponse response
  ) {
    if (limitReached) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return CompletableFuture.completedFuture(Maps.newHashMap());
    }
    var processes = Lists.<CompletableFuture<Void>>newArrayList();
    var entry = TableEntry.create(owner, tableId, creator, name,
      System.currentTimeMillis(), 0);
    processes.add(tableDatabaseTable().insertTable(entry));
    var defaultColumns = Lists.<DatabaseColumn>newArrayList();
    defaultColumns.add(DatabaseColumn.create("owner", DatabaseDataType.UUID,
      DatabaseColumn.Type.PARTITION_KEY));
    defaultColumns.add(DatabaseColumn.create("id", DatabaseDataType.UUID,
      DatabaseColumn.Type.CLUSTERING_KEY));
    defaultColumns.add(DatabaseColumn.create("data", DatabaseDataType.TEXT));
    var table = tableFactory.create(entry, defaultColumns);
    processes.add(table.createAsyncIfNotExists()
      .thenCompose(value -> table.createIndexAsyncIfNotExists("id"))
      .thenCompose(value -> table.createIndexAsyncIfNotExists("data")));
    return AsyncIterator.execute(processes, process -> process)
      .thenApply(value -> Map.of("table", tableId));
  }

  @RequestMapping(path = "/table/entry/insert/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> insertTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), tableId,
      tableEntry -> tableFactory.create(tableEntry)
        .thenAccept(table -> table.generateAvailableContentId()
          .thenAccept(contentId -> insertTableEntry(tableEntry, table, contentId)
            .thenAccept(futureResponse::complete))), () -> {});
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> insertTableEntry(
    TableEntry tableEntry, Table table, UUID contentId
  ) {
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("owner", tableEntry.owner()));
    cells.add(TableCell.create("id", contentId));
    for (var column : table.columns()) {
      if (column.name().equals("owner") || column.name().equals("id")) {
        continue;
      }
      cells.add(TableCell.create(column.name(), ""));
    }
    workflowModule.triggerWorkflows("table", "database-entry-insert-trigger",
      DatabaseCondition.of("tableId", tableEntry.id()),
      tableInsertInformation(table, contentId));
    return table.insertContent(TableRow.create(errorRepository, cells))
      .thenApply(success -> finishTableEntryInsertion(success, contentId));
  }

  private Map<String, Object> finishTableEntryInsertion(
    boolean success, UUID contentId
  ) {
    if (!success) {
      return Map.of("success", false);
    }
    return  Map.of("success", true, "id", contentId);
  }

  private Map<String, Object> tableInsertInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  @RequestMapping(path = "/table/entry/update/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> updateTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var rowId = body.getUUID("row");
    var column = body.getString("column");
    var value = body.getString("value");
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), tableId,
      tableEntry -> tableFactory.create(tableEntry)
        .thenAccept(table -> table.findContent(rowId)
          .thenAccept(row -> updateTableEntry(table, row, rowId, column, value)
            .thenAccept(futureResponse::complete))), () -> {});
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> updateTableEntry(
    Table table, DatabaseRow row, UUID rowId, String cellColumn, String cellValue
  ) {
    var columns = table.columns();
    if (columns.stream().noneMatch(entry -> entry.name().equals(cellColumn))) {
      return CompletableFuture.completedFuture(Map.of("success", false));
    }
    var cells = Lists.<TableCell>newArrayList();
    for (var i = 0; i < columns.size(); i++) {
      var column = columns.get(i).name();
      if (column.equals(cellColumn)) {
        cells.add(TableCell.create(cellColumn, cellValue));
        continue;
      }
      cells.add(TableCell.create(column, row.findCell(i).value()));
    }
    return table.updateContent(rowId, TableRow.create(errorRepository, cells))
      .thenApply(success -> Map.of("success", success));
  }

  @RequestMapping(path = "/table/entry/remove/", method = RequestMethod.POST)
  public void removeTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    performTableOperation(findUserId(request), body.getString("table"),
      tableEntry -> tableFactory.create(tableEntry).thenAccept(table ->
        removeTableEntry(tableEntry, table, body.getUUID("row"))), () -> {});
  }

  private void removeTableEntry(TableEntry tableEntry, Table table, UUID rowId) {
    table.removeContent(rowId);
    workflowModule.triggerWorkflows("table", "database-entry-remove-trigger",
      DatabaseCondition.of("tableId", tableEntry.id()),
      tableRemoveInformation(table, rowId));
  }

  private Map<String, Object> tableRemoveInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  private static final Pattern COLUMN_PATTERN =
    Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

  @RequestMapping(path = "/table/column/add/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> addTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName", 64);
    if (columnName.equalsIgnoreCase("id") || columnName.equalsIgnoreCase("owner")) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1000));
    }
    if (!COLUMN_PATTERN.matcher(columnName).matches()) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1001));
    }
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), tableId,
      tableEntry -> tableFactory.create(tableEntry)
        .thenAccept(table -> addTableColumn(table, columnName)
          .thenAccept(futureResponse::complete)),
      () -> futureResponse.complete(Map.of("success", false)));
    return futureResponse;
  }

  private static final int MAX_TABLE_COLUMNS = 20;

  private CompletableFuture<Map<String, Object>> addTableColumn(
    Table table, String columnName
  ) {
    if (table.columns().stream().anyMatch(column -> column.name().equals(columnName))) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1002));
    }
    if ((table.columns().size() - 2) + 1 > MAX_TABLE_COLUMNS) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1003));
    }
    return table.addColumn(DatabaseColumn.create(columnName, DatabaseDataType.TEXT))
      .thenCompose(value -> table.createIndexAsyncIfNotExists(columnName))
      .thenApply(value -> Map.of("success", true));
  }

  @RequestMapping(path = "/table/column/remove/", method = RequestMethod.POST)
  public CompletableFuture<Void> removeTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName");
    if (columnName.equalsIgnoreCase("id") || columnName.equalsIgnoreCase("owner")) {
      return CompletableFuture.completedFuture(null);
    }
    var futureResponse = new CompletableFuture<Void>();
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableEntry).thenAccept(table ->
        removeTableColumn(table, columnName).thenAccept(futureResponse::complete)),
      () -> {});
    return futureResponse;
  }

  private CompletableFuture<Void> removeTableColumn(Table table, String columnName) {
    if (table.columns().stream().noneMatch(column -> column.name().equals(columnName))) {
      return CompletableFuture.completedFuture(null);
    }
    return table.dropIndexAsyncIfExists(columnName)
      .thenCompose(value -> table.dropColumn(columnName));
  }

  @RequestMapping(path = "/table/rename/", method = RequestMethod.POST)
  public void renameTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var tableName = body.getString("name", 64);
    if (tableName.replace(" ", "").isEmpty()) {
      return;
    }
    performTableOperation(findUserId(request), tableId, table ->
      tableDatabaseTable().changeTableName(table, tableName), () -> {});
  }

  @RequestMapping(path = "/table/delete/", method = RequestMethod.POST)
  public void deleteTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    performTableOperation(findUserId(request), body.getString("table"),
      this::deleteTable, () -> {});
  }

  public void deleteTable(TableEntry tableEntry) {
    var tableId = tableEntry.id();
    tableDatabaseTable().deleteTable(tableId);
    tableFactory.create(tableEntry).thenAccept(DatabaseTable::dropIfExists);
  }
}
