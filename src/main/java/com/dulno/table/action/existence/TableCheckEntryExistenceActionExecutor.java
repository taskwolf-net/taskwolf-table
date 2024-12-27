package com.dulno.table.action.existence;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.workflow.action.ActionExecutor;
import com.dulno.workflow.action.ActionResult;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.workflow.placeholder.PlaceholderDissolve;
import com.dulno.table.structure.Table;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableCheckEntryExistenceActionExecutor implements ActionExecutor {
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
      return ActionResult.futureFailure("table.action.check.entry.existence.failure.table.not.found");
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
      return ActionResult.futureFailure("table.action.check.entry.existence.failure.column.not.found");
    }
    return table.exists(DatabaseCondition.of(entryColumn, entryValue))
      .thenApply(exists -> ActionResult.success(buildInformation(table, exists)));
  }

  private Map<String, Object> buildInformation(Table table, boolean exists) {
    var information = Maps.<String, Object>newHashMap();
    information.put("entryExists", exists);
    information.put("tableName", table.name());
    information.put("entryColumn", entryColumn);
    information.put("entryValue", entryValue);
    return information;
  }
}
