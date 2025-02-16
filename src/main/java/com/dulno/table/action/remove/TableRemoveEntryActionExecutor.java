package com.dulno.table.action.remove;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
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
public final class TableRemoveEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final UUID ownerId;
  private final String tableIdentifier;
  private String entryIdentifier;

  @Override
  public CompletableFuture<ActionResult> execute(Map<String, Object> information) {
    return tableDatabaseTable.tableExists(tableIdentifier).thenCompose(exists ->
      execute(information, exists));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, boolean tableExists
  ) {
    if (!tableExists) {
      return ActionResult.futureFailure("table.action.entry.remove.failure.table.not.found");
    }
    return tableDatabaseTable.findTable(tableIdentifier)
      .thenCompose(tableEntry -> execute(information, tableEntry));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry
  ) {
    if (!tableEntry.owner().equals(ownerId)) {
      return ActionResult.futureFailure("table.action.entry.remove.failure.table.permission");
    }
    return tableFactory.create(tableEntry)
      .thenCompose(table -> execute(information, table));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, Table table
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryIdentifier = placeholderDissolve.dissolve(entryIdentifier);
    try {
      var entryId = UUID.fromString(entryIdentifier);
      return table.contentExists(entryId).thenApply(exists ->
        execute(table, entryId, exists));
    } catch (Exception exception) {
      return ActionResult.futureFailure("table.action.entry.remove.failure.entry.wrong.format");
    }
  }

  private ActionResult execute(Table table, UUID entryId, boolean entryExists) {
    if (!entryExists) {
      return ActionResult.failure("table.action.entry.remove.failure.entry.not.found");
    }
    table.removeContent(entryId);
    return ActionResult.success(buildInformation(table));
  }

  private Map<String, Object> buildInformation(Table table) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    return information;
  }
}
