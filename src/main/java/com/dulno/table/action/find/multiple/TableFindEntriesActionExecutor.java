package com.dulno.table.action.find.multiple;

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
import org.json.JSONArray;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFindEntriesActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final UUID ownerId;
  private final String tableIdentifier;
  private String entriesColumn;
  private String entriesValue;

  @Override
  public CompletableFuture<ActionResult> execute(Map<String, Object> information) {
    return tableDatabaseTable.tableExists(tableIdentifier).thenCompose(exists ->
      execute(information, exists));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, boolean tableExists
  ) {
    if (!tableExists) {
      return ActionResult.futureFailure("table.action.entries.find.failure.table.not.found");
    }
    return tableDatabaseTable.findTable(tableIdentifier)
      .thenCompose(tableEntry -> execute(information, tableEntry));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry
  ) {
    if (!tableEntry.owner().equals(ownerId)) {
      return ActionResult.futureFailure("table.action.entries.find.failure.table.permission");
    }
    return tableFactory.create(tableEntry)
      .thenCompose(table -> execute(information, table));
  }

  private static final long ENTRY_LIMIT = 100;

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, Table table
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entriesColumn = placeholderDissolve.dissolve(entriesColumn);
    entriesValue = placeholderDissolve.dissolve(entriesValue);
    if (table.columns().stream().noneMatch(column -> column.name().equals(entriesColumn))) {
      return ActionResult.futureFailure("table.action.entries.find.failure.column.not.found");
    }
    var condition = DatabaseCondition.of(entriesColumn, entriesValue);
    return table.selectRows(condition, ENTRY_LIMIT)
      .thenApply(rows -> ActionResult.success(buildInformation(rows.stream()
        .map(row -> buildRowInformation(table, row)).toList())));
  }

  private Map<String, Object> buildRowInformation(Table table, DatabaseRow row) {
    var information = Maps.<String, Object>newHashMap();
    var columns = table.tableColumns();
    information.put("database_column_id", row.findCell(2).uuidValue());
    for (var i = 0; i < columns.size(); i++) {
      information.put("database_column_" + columns.get(i).id(),
        row.findCell(i + 3).rawValue());
    }
    return information;
  }

  private Map<String, Object> buildInformation(List<Map<String, Object>> entries) {
    var information = Maps.<String, Object>newHashMap();
    information.put("entries", new JSONArray(entries));
    information.put("entriesNumber", entries.size());
    return information;
  }
}
