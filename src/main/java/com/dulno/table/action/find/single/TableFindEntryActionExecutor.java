package com.dulno.table.action.find.single;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.action.ActionExecutor;
import com.dulno.core.action.ActionResult;
import com.dulno.core.database.DatabaseRow;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.workflow.placeholder.PlaceholderDissolve;
import com.dulno.table.structure.Table;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFindEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
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
      .thenCompose(tableEntry -> tableFactory.create(tableEntry)
        .thenCompose(table -> execute(information, table)));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, Table table
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryColumn = placeholderDissolve.dissolve(entryColumn);
    entryValue = placeholderDissolve.dissolve(entryValue);
    if (table.columns().stream().noneMatch(column -> column.name().equals(entryColumn))) {
      return ActionResult.futureFailure("table.action.entry.find.failure.column.not.found");
    }
    var condition = DatabaseCondition.of(entryColumn, entryValue);
    return table.exists(condition).thenCompose(exists ->
      execute(table, condition, exists));
  }

  private CompletableFuture<ActionResult> execute(
    Table table, DatabaseCondition condition, boolean exists
  ) {
    if (!exists) {
      return ActionResult.futureFailure("table.action.entry.find.failure.entry.not.found");
    }
    return table.selectRow(condition).thenApply(row ->
      ActionResult.success(buildInformation(table, row.findCell(1).uuidValue(),
        parseRowContent(table, row))));
  }

  private String parseRowContent(Table table, DatabaseRow row) {
    var columns = table.columns();
    var content = new StringBuilder();
    for (var i = 2; i < columns.size(); i++) {
      content.append(columns.get(i).name());
      content.append("=");
      content.append(row.findCell(i).rawValue());
      if (i < columns.size() - 1) {
        content.append(",");
      }
    }
    return content.toString();
  }

  private Map<String, Object> buildInformation(Table table, UUID id, String row) {
    var information = Maps.<String, Object>newHashMap();
    information.put("entryId", id);
    information.put("entryContent", row);
    information.put("tableName", table.name());
    information.put("entryColumn", entryColumn);
    information.put("entryValue", entryValue);
    return information;
  }
}
