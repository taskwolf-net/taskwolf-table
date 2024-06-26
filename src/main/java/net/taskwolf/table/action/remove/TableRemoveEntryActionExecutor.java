package net.taskwolf.table.action.remove;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import lombok.AllArgsConstructor;
import net.taskwolf.core.action.ActionExecutor;
import net.taskwolf.core.action.ActionResult;
import net.taskwolf.core.workflow.placeholder.PlaceholderDissolve;
import net.taskwolf.table.structure.Table;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableRemoveEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
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
    return tableDatabaseTable.findTable(tableIdentifier).thenCompose(tableEntry ->
      tableFactory.create(tableEntry).thenCompose(table ->
        execute(information, table)));
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
