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
import com.dulno.core.database.DatabaseColumn;
import com.dulno.core.database.DatabaseDataType;
import com.dulno.core.database.DatabaseTable;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.organization.team.TeamTargetDatabaseTable;
import com.dulno.core.user.UserDatabaseTable;
import com.dulno.core.user.UserTargetDatabaseTable;
import org.springframework.web.bind.annotation.*;

import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

@RestController
public final class TableModificationController extends TableController {
  private final TableFactory tableFactory;
  private final WorkflowModule workflowModule;
  private final ErrorRepository errorRepository;

  private TableModificationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable,
    TableFactory tableFactory, WorkflowModule workflowModule,
    ErrorRepository errorRepository
  ) {
    super(secretKey, userDatabaseTable, tableDatabaseTable,
      userTargetDatabaseTable, teamTargetDatabaseTable);
    this.tableFactory = tableFactory;
    this.workflowModule = workflowModule;
    this.errorRepository = errorRepository;
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
    cells.add(TableCell.create("timestamp", System.currentTimeMillis()));
    cells.add(TableCell.create("id", contentId));
    for (var column : table.columns()) {
      if (column.name().equals("owner") || column.name().equals("timestamp") ||
        column.name().equals("id")
      ) {
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
    var value = body.getAny("value");
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), tableId,
      tableEntry -> tableFactory.create(tableEntry)
        .thenAccept(table -> table.findContent(rowId)
          .thenAccept(row -> updateTableEntry(table, row, rowId, column, value)
            .thenAccept(futureResponse::complete))), () -> {});
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> updateTableEntry(
    Table table, DatabaseRow row, UUID rowId, String cellColumn, Object cellValue
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

  @RequestMapping(path = "/table/entries/remove/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> removeTableEntries(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), body.getString("table"),
      tableEntry -> tableFactory.create(tableEntry)
        .thenAccept(table -> table.findTableBundleOwner()
          .thenAccept(bundleOwner -> removeTableEntries(tableEntry, table,
            bundleOwner, body.getList("rows")).thenAccept(futureResponse::complete))),
      () -> {});
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> removeTableEntries(
    TableEntry tableEntry, Table table, UUID tableBundleOwner, List<String> rowIds
  ) {
    var executorService = Executors.newSingleThreadExecutor();
    for (var rowId : rowIds) {
      executorService.submit(() -> removeTableEntry(tableEntry, table,
        tableBundleOwner, UUID.fromString(rowId)).join());
    }
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    executorService.submit(() -> futureResponse.complete(Map.of("success", true)));
    executorService.shutdown();
    return futureResponse;
  }

  private CompletableFuture<Void> removeTableEntry(
    TableEntry tableEntry, Table table, UUID tableBundleOwner, UUID rowId
  ) {
    workflowModule.triggerWorkflows("table", "database-entry-remove-trigger",
      DatabaseCondition.of("tableId", tableEntry.id()),
      tableRemoveInformation(table, rowId));
    return table.removeContent(rowId, tableBundleOwner);
  }

  private Map<String, Object> tableRemoveInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  @RequestMapping(path = "/table/clear/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> clearTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), body.getString("table"),
      tableEntry -> tableFactory.create(tableEntry)
        .thenAccept(table -> table.truncate()
          .thenAccept(value -> futureResponse.complete(Map.of("success", true)))),
      () -> {});
    return futureResponse;
  }

  @RequestMapping(path = "/table/column/add/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> addTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName", 64);
    var columnType = TableColumnType.valueOf(body.getString("columnType"));
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), tableId,
      tableEntry -> tableFactory.create(tableEntry)
        .thenAccept(table -> addTableColumn(table, columnName, columnType)
          .thenAccept(futureResponse::complete)),
      () -> futureResponse.complete(Map.of("success", false)));
    return futureResponse;
  }

  private static final int MAX_TABLE_COLUMNS = 20;

  private CompletableFuture<Map<String, Object>> addTableColumn(
    Table table, String columnName, TableColumnType columnType
  ) {
    if (table.tableColumns().size() + 1 > MAX_TABLE_COLUMNS) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1000));
    }
    return table.addColumn(columnName, columnType)
      .thenApply(value -> Map.of("success", true));
  }

  @RequestMapping(path = "/table/column/remove/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> removeTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnId = body.getString("column");
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableEntry).thenAccept(table ->
        removeTableColumn(table, columnId).thenAccept(futureResponse::complete)),
      () -> {});
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> removeTableColumn(
    Table table, String columnId
  ) {
    if (table.tableColumns().stream().noneMatch(column -> column.id().equals(columnId))) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1000));
    }
    return table.dropColumn(columnId).thenApply(value -> Map.of("success", true));
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
