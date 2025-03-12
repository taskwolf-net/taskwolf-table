package com.dulno.table.action.flip;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.database.DatabaseRow;
import com.dulno.core.error.ErrorRepository;
import com.dulno.table.structure.*;
import com.dulno.workflow.action.ActionExecutor;
import com.dulno.workflow.action.ActionResult;
import com.dulno.workflow.placeholder.PlaceholderDissolve;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFlipEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ErrorRepository errorRepository;
  private final UUID ownerId;
  private final String tableIdentifier;
  private final String flipColumn;
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
      return ActionResult.futureFailure("table.action.entry.flip.failure.table.not.found");
    }
    return tableDatabaseTable.findTable(tableIdentifier)
      .thenCompose(tableEntry -> execute(information, tableEntry));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry
  ) {
    if (!tableEntry.owner().equals(ownerId)) {
      return ActionResult.futureFailure("table.action.entry.flip.failure.table.permission");
    }
    return tableFactory.create(tableEntry)
      .thenCompose(table -> execute(information, table));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, Table table
  ) {
    if (table.tableColumns().stream().noneMatch(column -> column.id().equals(flipColumn))) {
      return ActionResult.futureFailure("table.action.entry.flip.failure.column.not.found");
    }
    var column = table.tableColumns().stream()
      .filter(tableColumn -> tableColumn.id().equals(flipColumn)).findFirst().get();
    if (column.type() != TableColumnType.SWITCH && column.type() != TableColumnType.CHECKBOX) {
      return ActionResult.futureFailure("table.action.entry.flip.failure.column.type");
    }
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryIdentifier = placeholderDissolve.dissolve(entryIdentifier);
    try {
      var entryId = UUID.fromString(entryIdentifier);
      return table.contentExists(entryId).thenCompose(exists ->
        execute(table, entryId, exists));
    } catch (Exception exception) {
      return ActionResult.futureFailure("table.action.entry.flip.failure.entry.wrong.format");
    }
  }

  private CompletableFuture<ActionResult> execute(
    Table table, UUID entryId, boolean entryExists
  ) {
    if (!entryExists) {
      return ActionResult.futureFailure("table.action.entry.flip.failure.entry.not.found");
    }
    return table.findContent(entryId)
      .thenApply(row -> createCells(table, row))
      .thenCompose(cells -> table.updateContent(entryId,
          TableRow.create(errorRepository, cells))
        .thenApply(success -> success ? ActionResult.success(Maps.newHashMap()) :
          ActionResult.failure("table.action.entry.flip.failure.data.limit.reached")));
  }

  private List<TableCell> createCells(
    Table table, DatabaseRow row
  ) {
    var tableColumns = table.columns();
    var cells = Lists.<TableCell>newArrayList();
    for (var i = 0; i < tableColumns.size(); i++) {
      var column = tableColumns.get(i);
      var value = row.findCell(i).value();
      if (column.name().equals(flipColumn)) {
        cells.add(TableCell.create(column.name(),
          value == null ? true : !((boolean) value)));
      } else {
        cells.add(TableCell.create(column.name(), value));
      }
    }
    return cells;
  }
}
