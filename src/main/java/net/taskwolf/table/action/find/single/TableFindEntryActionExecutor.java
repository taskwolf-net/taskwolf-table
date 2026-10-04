package net.taskwolf.table.action.find.single;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import net.taskwolf.core.database.DatabaseDataType;
import net.taskwolf.core.database.DatabaseRow;
import net.taskwolf.core.database.condition.DatabaseCondition;
import net.taskwolf.table.structure.*;
import net.taskwolf.workflow.action.ActionExecutor;
import net.taskwolf.workflow.action.ActionResult;
import net.taskwolf.workflow.placeholder.PlaceholderDissolve;
import lombok.AllArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFindEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final UUID ownerId;
  private final String tableIdentifier;
  private String entryColumn;
  private String entryValue;

  @Override
  public CompletableFuture<ActionResult> execute(Map<String, Object> information) {
    return tableDatabaseTable.tableExists(tableIdentifier).thenCompose(exists ->
      execute(information, exists));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, boolean tableExists
  ) {
    if (!tableExists) {
      return ActionResult.futureFailure("table.action.entry.find.failure.table.not.found");
    }
    return tableDatabaseTable.findTable(tableIdentifier)
      .thenCompose(tableEntry -> execute(information, tableEntry));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry
  ) {
    if (!tableEntry.owner().equals(ownerId)) {
      return ActionResult.futureFailure("table.action.entry.find.failure.table.permission");
    }
    return tableFactory.create(tableEntry)
      .thenCompose(table -> execute(information, table));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, Table table
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryColumn = placeholderDissolve.dissolve(entryColumn);
    entryValue = placeholderDissolve.dissolve(entryValue);
    if (table.tableColumns().stream().noneMatch(column -> column.id().equals(entryColumn)) &&
      !entryColumn.equals("id")
    ) {
      return ActionResult.futureFailure("table.action.entry.find.failure.column.not.found");
    }
    try {
      var condition = createDatabaseCondition(table);
      return table.exists(condition).thenCompose(exists ->
        execute(table, condition, exists));
    } catch (Exception exception) {
      return ActionResult.futureFailure("table.action.entry.find.failure.entry.wrong.format");
    }
  }

  private CompletableFuture<ActionResult> execute(
    Table table, DatabaseCondition condition, boolean exists
  ) {
    if (!exists) {
      return ActionResult.futureFailure("table.action.entry.find.failure.entry.not.found");
    }
    return table.selectRow(condition).thenApply(row ->
      ActionResult.success(buildInformation(table, row)));
  }

  private DatabaseCondition createDatabaseCondition(Table table) {
    if (entryColumn.equals("id")) {
      return DatabaseCondition.of(entryColumn, UUID.fromString(entryValue));
    }
    var column = table.tableColumns().stream()
      .filter(tableColumn -> tableColumn.id().equals(entryColumn)).findFirst().get();
    if (column.type().dataType() == DatabaseDataType.BOOLEAN) {
      return DatabaseCondition.of(entryColumn, Boolean.parseBoolean(entryValue));
    } else if (column.type().dataType() == DatabaseDataType.DECIMAL) {
      return DatabaseCondition.of(entryColumn, new BigDecimal(entryValue));
    }
    return DatabaseCondition.of(entryColumn, entryValue);
  }

  private Map<String, Object> buildInformation(Table table, DatabaseRow row) {
    var information = Maps.<String, Object>newHashMap();
    var columns = table.tableColumns();
    information.put("database_column_id", row.findCell(2).uuidValue());
    for (var i = 0; i < columns.size(); i++) {
      var column = columns.get(i);
      var value = row.findCell(i + 3).rawValue();
      if (value == null) {
        value = findDefaultValue(column);
      }
      information.put("database_column_" + column.id(), value);
    }
    return information;
  }

  private Object findDefaultValue(TableColumn column) {
    if (column.type().dataType() == DatabaseDataType.BOOLEAN) {
      return false;
    } else if (column.type().dataType() == DatabaseDataType.DECIMAL) {
      return 0;
    }
    return "";
  }
}
