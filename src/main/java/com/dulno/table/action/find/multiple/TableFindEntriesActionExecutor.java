package com.dulno.table.action.find.multiple;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.action.ActionExecutor;
import com.dulno.core.action.ActionResult;
import com.dulno.core.database.DatabaseRow;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.workflow.placeholder.PlaceholderDissolve;
import com.dulno.table.structure.Table;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.google.common.collect.Lists;
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
      .thenCompose(tableEntry -> tableFactory.create(tableEntry)
        .thenCompose(table -> execute(information, table)));
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
      .thenApply(rows -> ActionResult.success(buildInformation(table,
        rows.stream()
          .map(row -> buildRowInformation(row.findCell(1).uuidValue(),
            createCells(table, row)))
          .toList())));
  }

  private List<Map<String, Object>> createCells(
    Table table, DatabaseRow row
  ) {
    var cells = Lists.<Map<String, Object>>newArrayList();
    var columns = table.columns();
    for (var i = 2; i < columns.size(); i++) {
      cells.add(Map.of("entryCellColumn", columns.get(i).name().toLowerCase(),
        "entryCellValue", row.findCell(i).rawValue()));
    }
    return cells;
  }

  private Map<String, Object> buildRowInformation(
    UUID id, List<Map<String, Object>> cells
  ) {
    var information = Maps.<String, Object>newHashMap();
    information.put("entryId", id);
    information.put("entryCells", new JSONArray(cells));
    information.put("entryCellsNumber", cells.size());
    return information;
  }

  private Map<String, Object> buildInformation(
    Table table, List<Map<String, Object>> entries
  ) {
    var information = Maps.<String, Object>newHashMap();
    information.put("entries", new JSONArray(entries));
    information.put("entriesNumber", entries.size());
    information.put("tableName", table.name());
    information.put("entriesColumn", entriesColumn);
    information.put("entriesValue", entriesValue);
    return information;
  }
}
