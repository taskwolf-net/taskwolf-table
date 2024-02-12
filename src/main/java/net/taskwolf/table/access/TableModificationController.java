package net.taskwolf.table.access;

import com.google.common.collect.Lists;
import jakarta.servlet.http.HttpServletRequest;
import net.taskwolf.core.access.TaskwolfRestController;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseDataType;
import net.taskwolf.core.database.DatabaseTable;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.*;
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

  private TableModificationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    UserTargetDatabaseTable userTargetDatabaseTable
  ) {
    super(secretKey, userDatabaseTable);
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableFactory = tableFactory;
    this.userTargetDatabaseTable = userTargetDatabaseTable;
  }

  @RequestMapping(path = "/table/create/", method = RequestMethod.POST)
  public void createTable(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableName = (String) input.get("name");
    var userId = findUserId(request);
    userTargetDatabaseTable.findTarget(userId).thenAccept(target ->
      tableDatabaseTable.generateAvailableTableId().thenAccept(tableId ->
        createTable(tableId, target, userId, tableName)));
  }

  private void createTable(String tableId, UUID owner, UUID creator, String name) {
    tableDatabaseTable.insertTable(tableId, owner, creator, name,
      System.currentTimeMillis());
    var defaultColumn = DatabaseColumn.create("id", DatabaseDataType.UUID,
      DatabaseColumn.Type.PRIMARY_KEY);
    var table = tableFactory.create(tableId, Lists.newArrayList(defaultColumn));
    table.createIfNotExists();
  }

  @RequestMapping(path = "/table/entry/insert/", method = RequestMethod.POST)
  public void insertTableEntry(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableId = (String) input.get("table");
    var rowContent = (Map<String, Object>) input.get("row");
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        insertTableEntry(table, rowContent)));
  }

  private void insertTableEntry(Table table, Map<String, Object> rowContent) {
    var cells = Lists.<TableCell>newArrayList();
    for (var entry : rowContent.entrySet()) {
      cells.add(TableCell.create(entry.getKey(), entry.getValue()));
    }
    table.insertContent(TableRow.create(cells));
  }

  @RequestMapping(path = "/table/entry/remove/", method = RequestMethod.POST)
  public void removeTableEntry(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableId = (String) input.get("table");
    var rowId =  UUID.fromString((String) input.get("row"));
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        table.removeContent(rowId)));
  }

  @RequestMapping(path = "/table/column/add/", method = RequestMethod.POST)
  public void addTableColumn(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableId = (String) input.get("table");
    var columnName = (String) input.get("columnName");
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
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableId = (String) input.get("table");
    var columnName = (String) input.get("columnName");
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

  @RequestMapping(path = "/table/column/rename/", method = RequestMethod.POST)
  public void renameTableColumn(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableId = (String) input.get("table");
    var oldColumnName = (String) input.get("oldColumnName");
    var newColumnName = (String) input.get("newColumnName");
    if (oldColumnName.equalsIgnoreCase("id") || newColumnName.equalsIgnoreCase("id")) {
      return;
    }
    if (newColumnName.replace(" ", "").isEmpty()) {
      return;
    }
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableId).thenAccept(table ->
        renameTableColumn(table, oldColumnName, newColumnName)));
  }

  private void renameTableColumn(
    Table table, String oldColumnName, String newColumnName
  ) {
    if (table.columns().stream().noneMatch(column -> column.name().equals(oldColumnName))) {
      return;
    }
    if (table.columns().stream().anyMatch(column -> column.name().equals(newColumnName))) {
      return;
    }
    table.renameColumn(oldColumnName, newColumnName);
  }

  @RequestMapping(path = "/table/rename/", method = RequestMethod.POST)
  public void renameTable(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableId = (String) input.get("table");
    var tableName = (String) input.get("name");
    if (tableName.replace(" ", "").isEmpty()) {
      return;
    }
    performTableOperation(findUserId(request), tableId, table ->
      tableDatabaseTable.changeTableName(table.id(), tableName));
  }

  @RequestMapping(path = "/table/delete/", method = RequestMethod.POST)
  public void deleteTable(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var tableId = (String) input.get("table");
    performTableOperation(findUserId(request), tableId, this::deleteTable);
  }

  private void deleteTable(TableEntry tableEntry) {
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
