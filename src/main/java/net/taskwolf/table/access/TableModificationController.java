package net.taskwolf.table.access;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.taskwolf.core.CoreModule;
import net.taskwolf.core.access.TaskwolfRequestBody;
import net.taskwolf.core.access.TaskwolfRestController;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseDataType;
import net.taskwolf.core.database.DatabaseTable;
import net.taskwolf.core.trigger.TriggerEntry;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.*;
import net.taskwolf.table.trigger.insert.TableInsertEntryTrigger;
import net.taskwolf.table.trigger.remove.TableRemoveEntryTrigger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.security.Key;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

@RestController
public final class TableModificationController extends TaskwolfRestController {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final UserTargetDatabaseTable userTargetDatabaseTable;
  private final CoreModule coreModule;

  private TableModificationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    UserTargetDatabaseTable userTargetDatabaseTable, CoreModule coreModule
  ) {
    super(secretKey, userDatabaseTable);
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableFactory = tableFactory;
    this.userTargetDatabaseTable = userTargetDatabaseTable;
    this.coreModule = coreModule;
  }

  @RequestMapping(path = "/table/create/", method = RequestMethod.POST)
  public void createTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var userId = findUserId(request);
    var name = body.getString("name");
    if (name.isEmpty()) {
      return;
    }
    userTargetDatabaseTable.findTarget(userId).thenAccept(target ->
      tableDatabaseTable.generateAvailableTableId().thenAccept(tableId ->
        createTable(tableId, target, userId, name)));
  }

  private void createTable(String tableId, UUID owner, UUID creator, String name) {
    tableDatabaseTable.insertTable(tableId, owner, creator, name,
      System.currentTimeMillis());
    var defaultColumns = Lists.newArrayList(DatabaseColumn.create("id",
      DatabaseDataType.UUID, DatabaseColumn.Type.PRIMARY_KEY),
      DatabaseColumn.create("data", DatabaseDataType.TEXT));
    var table = tableFactory.create(tableId, defaultColumns);
    table.createIfNotExists();
  }

  @RequestMapping(path = "/table/entry/insert/", method = RequestMethod.POST)
  public void insertTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        table.generateAvailableContentId().thenAccept(contentId ->
          insertTableEntry(tableEntry, table, contentId,
            body.getObject("row").raw().toMap()))));
  }

  private void insertTableEntry(
    TableEntry tableEntry, Table table, UUID contentId,
    Map<String, Object> rowContent
  ) {
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("id", contentId));
    for (var entry : rowContent.entrySet()) {
      cells.add(TableCell.create(entry.getKey(), entry.getValue()));
    }
    table.insertContent(TableRow.create(cells));
    coreModule.triggerWorkflows("database", "database-entry-insert-trigger",
      "table='" + tableEntry.id() + "'", tableInsertInformation(table, contentId));
  }

  private Map<String, Object> tableInsertInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  @RequestMapping(path = "/table/entry/update/", method = RequestMethod.POST)
  public void updateTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        insertTableEntry(tableEntry, table, body.getUUID("row"),
          body.getObject("content").raw().toMap())));
  }

  private void updateTableEntry(
    Table table, UUID rowId, Map<String, Object> rowContent
  ) {
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("id", rowId));
    for (var entry : rowContent.entrySet()) {
      cells.add(TableCell.create(entry.getKey(), entry.getValue()));
    }
    table.updateContent(rowId, TableRow.create(cells));
  }

  @RequestMapping(path = "/table/entry/remove/", method = RequestMethod.POST)
  public void removeTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        removeTableEntry(tableEntry, table, body.getUUID("row"))));
  }

  private void removeTableEntry(TableEntry tableEntry, Table table, UUID rowId) {
    table.removeContent(rowId);
    coreModule.triggerWorkflows("database", "database-entry-remove-trigger",
      "table='" + tableEntry.id() + "'", tableRemoveInformation(table, rowId));
  }

  private Map<String, Object> tableRemoveInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  @RequestMapping(path = "/table/column/add/", method = RequestMethod.POST)
  public void addTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName");
    if (columnName.equalsIgnoreCase("id")) {
      return;
    }
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        addTableColumn(table, columnName)));
  }

  private void addTableColumn(Table table, String columnName) {
    if (table.columns().stream().anyMatch(column -> column.name().equals(columnName))) {
      return;
    }
    table.addColumn(DatabaseColumn.create(columnName, DatabaseDataType.TEXT));
  }

  @RequestMapping(path = "/table/column/remove/", method = RequestMethod.POST)
  public void removeTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName");
    if (columnName.equalsIgnoreCase("id")) {
      return;
    }
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        removeTableColumn(table, columnName)));
  }

  private void removeTableColumn(Table table, String columnName) {
    if (table.columns().stream().noneMatch(column -> column.name().equals(columnName))) {
      return;
    }
    table.dropColumn(columnName);
  }

  @RequestMapping(path = "/table/rename/", method = RequestMethod.POST)
  public void renameTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var tableName = body.getString("name");
    if (tableName.replace(" ", "").isEmpty()) {
      return;
    }
    performTableOperation(findUserId(request), tableId, table ->
      tableDatabaseTable.changeTableName(table.id(), tableName));
  }

  @RequestMapping(path = "/table/delete/", method = RequestMethod.POST)
  public void deleteTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    performTableOperation(findUserId(request), tableId, this::deleteTable);
  }

  public void deleteTable(TableEntry tableEntry) {
    var tableId = tableEntry.id();
    tableDatabaseTable.deleteTable(tableId);
    tableFactory.create(tableId).thenAccept(DatabaseTable::dropIfExists);
  }

  private void performTableOperation(
    UUID userId, String tableId, Consumer<TableEntry> operation
  ) {
    userTargetDatabaseTable.findTarget(userId).thenAccept(target ->
      tableDatabaseTable.tableExists(tableId).thenAccept(exists ->
        performTableOperation(target, tableId, exists, operation)));
  }

  private void performTableOperation(
    UUID target, String tableId, boolean tableExists, Consumer<TableEntry> operation
  ) {
    if (!tableExists) {
      return;
    }
    tableDatabaseTable.findTable(tableId).thenAccept(table ->
      performTableOperation(target, table, operation));
  }

  private void performTableOperation(
    UUID target, TableEntry table, Consumer<TableEntry> operation
  ) {
    if (!target.equals(table.owner())) {
      return;
    }
    operation.accept(table);
  }
}
