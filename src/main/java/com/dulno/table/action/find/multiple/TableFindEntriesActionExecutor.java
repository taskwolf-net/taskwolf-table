package com.dulno.table.action.find.multiple;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.database.DatabaseDataType;
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

import java.math.BigDecimal;
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
    if (table.tableColumns().stream().noneMatch(column -> column.id().equals(entriesColumn)) &&
      !entriesColumn.equals("id")
    ) {
      return ActionResult.futureFailure("table.action.entries.find.failure.column.not.found");
    }
    try {
      var condition = createDatabaseCondition(table);
      return table.selectRows(condition, ENTRY_LIMIT)
        .thenApply(rows -> ActionResult.success(buildInformation(rows.stream()
          .map(row -> buildRowInformation(table, row)).toList())));
    } catch (Exception exception) {
      return ActionResult.futureFailure("table.action.entries.find.failure.entry.wrong.format");
    }
  }

  private DatabaseCondition createDatabaseCondition(Table table) {
    if (entriesColumn.equals("id")) {
      return DatabaseCondition.of(entriesColumn, UUID.fromString(entriesValue));
    }
    var column = table.tableColumns().stream()
      .filter(tableColumn -> tableColumn.id().equals(entriesColumn)).findFirst().get();
    if (column.type().dataType() == DatabaseDataType.BOOLEAN) {
      return DatabaseCondition.of(entriesColumn, Boolean.parseBoolean(entriesValue));
    } else if (column.type().dataType() == DatabaseDataType.DECIMAL) {
      return DatabaseCondition.of(entriesColumn, new BigDecimal(entriesValue));
    }
    return DatabaseCondition.of(entriesColumn, entriesValue);
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
