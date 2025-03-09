package com.dulno.table.action.find.single;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.database.DatabaseRow;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.table.structure.Table;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableEntry;
import com.dulno.table.structure.TableFactory;
import com.dulno.workflow.action.ActionExecutor;
import com.dulno.workflow.action.ActionResult;
import com.dulno.workflow.placeholder.PlaceholderDissolve;
import lombok.AllArgsConstructor;

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
    if (table.columns().stream().noneMatch(column -> column.name().equals(entryColumn)) ||
      entryColumn.equalsIgnoreCase("owner")
    ) {
      return ActionResult.futureFailure("table.action.entry.find.failure.column.not.found");
    }
    try {
      var condition = DatabaseCondition.of(entryColumn,
        entryColumn.equalsIgnoreCase("id") ? UUID.fromString(entryValue) : entryValue);
      return table.exists(condition).thenCompose(exists ->
        execute(table, condition, exists));
    } catch (Exception exception) {
      return ActionResult.futureFailure("table.action.entry.find.failure.entry.not.found");
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

  private Map<String, Object> buildInformation(Table table, DatabaseRow row) {
    var information = Maps.<String, Object>newHashMap();
    var columns = table.tableColumns();
    information.put("database_column_id", row.findCell(2).uuidValue());
    for (var i = 0; i < columns.size(); i++) {
      information.put("database_column_" + columns.get(i).id(),
        row.findCell(i + 3).rawValue());
    }
    return information;
  }
}
