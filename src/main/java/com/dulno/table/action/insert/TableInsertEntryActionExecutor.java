package com.dulno.table.action.insert;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.error.ErrorRepository;
import com.dulno.table.structure.*;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import com.dulno.workflow.action.ActionExecutor;
import com.dulno.workflow.action.ActionResult;
import com.dulno.core.database.DatabaseColumn;
import com.dulno.workflow.placeholder.PlaceholderDissolve;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableInsertEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ErrorRepository errorRepository;
  private final UUID ownerId;
  private final String tableIdentifier;
  private String entryContent;

  @Override
  public CompletableFuture<ActionResult> execute(Map<String, Object> information) {
    return tableDatabaseTable.tableExists(tableIdentifier).thenCompose(exists ->
      execute(information, exists));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, boolean tableExists
  ) {
    if (!tableExists) {
      return ActionResult.futureFailure("table.action.entry.insert.failure.table.not.found");
    }
    return tableDatabaseTable.findTable(tableIdentifier)
      .thenCompose(tableEntry -> execute(information, tableEntry));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry
  ) {
    if (!tableEntry.owner().equals(ownerId)) {
      return ActionResult.futureFailure("table.action.entry.insert.failure.table.permission");
    }
    return tableFactory.create(tableEntry)
      .thenCompose(table -> table.generateAvailableContentId()
        .thenCompose(contentId -> execute(information, tableEntry, table, contentId)));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry, Table table, UUID contentId
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryContent = placeholderDissolve.dissolve(entryContent);
    var cells = createCells(tableEntry, table, contentId);
    return table.insertContent(TableRow.create(errorRepository, cells))
      .thenApply(success -> success ? ActionResult.success(
        buildInformation(table, contentId)) :
        ActionResult.failure("table.action.entry.insert.failure.data.limit.reached"));
  }

  private List<TableCell> createCells(
    TableEntry tableEntry, Table table, UUID contentId
  ) {
    var content = new JSONObject(entryContent);
    var tableColumns = table.columns().stream().map(DatabaseColumn::name).toList();
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("owner", tableEntry.owner()));
    cells.add(TableCell.create("id", contentId));
    for (var column : tableColumns) {
      if (column.equals("id") || column.equals("owner")) {
        continue;
      }
      cells.add(TableCell.create(column, content.has(column) ?
        content.getString(column) : ""));
    }
    return cells;
  }

  private Map<String, Object> buildInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }
}
