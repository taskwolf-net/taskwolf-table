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
    return table.selectRows(condition)
      .thenApply(rows -> ActionResult.success(buildInformation(table,
        rows.stream()
          .map(row -> buildRowInformation(row.findCell(1).uuidValue(),
            parseRowContent(table, row)))
          .toList())));
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

  private Map<String, Object> buildRowInformation(UUID id, String row) {
    var information = Maps.<String, Object>newHashMap();
    information.put("entryId", id);
    information.put("entryContent", row);
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
