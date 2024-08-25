package net.taskwolf.table.action.insert;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import net.taskwolf.core.action.ActionExecutor;
import net.taskwolf.core.action.ActionResult;
import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.workflow.placeholder.PlaceholderDissolve;
import net.taskwolf.table.structure.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableInsertEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
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
    return tableDatabaseTable.findTable(tableIdentifier).thenCompose(tableEntry ->
      tableFactory.create(tableEntry).thenCompose(table ->
        table.generateAvailableContentId().thenCompose(contentId ->
          execute(information, tableEntry, table, contentId))));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry, Table table, UUID contentId
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryContent = placeholderDissolve.dissolve(entryContent);
    try {
      var cells = createCells(tableEntry, table, contentId);
      return table.insertContent(TableRow.create(cells)).thenApply(success ->
        success ? ActionResult.success(buildInformation(table, contentId)) :
          ActionResult.failure("table.action.entry.insert.failure.data.limit.reached"));
    } catch (Exception exception) {
      return ActionResult.futureFailure(exception.getMessage());
    }
  }

  private List<TableCell> createCells(
    TableEntry tableEntry, Table table, UUID contentId
  ) throws Exception {
    var tableColumns = table.columns().stream().map(DatabaseColumn::name).toList();
    var entries = entryContent.split(",");
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("owner", tableEntry.owner()));
    cells.add(TableCell.create("id", contentId));
    for (var entry : entries) {
      cells.add(createCell(entry, cells, tableColumns));
    }
    cells.addAll(createUncoveredCells(cells, tableColumns));
    return cells;
  }

  private TableCell createCell(
    String entry, List<TableCell> currentCells, List<String> columns
  ) throws Exception {
    if (!entry.contains("=")) {
      throw new Exception("table.action.entry.insert.failure.wrong.schema");
    }
    var split = entry.split("=");
    if (split.length != 2) {
      throw new Exception("table.action.entry.insert.failure.wrong.schema");
    }
    var column = split[0].replace(" ", "");
    var columnExists = columns.stream().anyMatch(tableColumn ->
      tableColumn.equalsIgnoreCase(column));
    if (!columnExists) {
      throw new Exception("table.action.entry.insert.failure.column.not.found");
    }
    if (currentCells.stream().anyMatch(cell -> cell.column().equalsIgnoreCase(column))) {
      throw new Exception("table.action.entry.insert.failure.column.already.specified");
    }
    var value = split[1];
    return TableCell.create(column.toLowerCase(), value);
  }

  private List<TableCell> createUncoveredCells(
    List<TableCell> cells, List<String> columns
  ) {
    var uncoveredCells = Lists.<TableCell>newArrayList();
    for (var column : columns) {
      var isColumnCovered = cells.stream().anyMatch(cell ->
        cell.column().equalsIgnoreCase(column));
      if (!isColumnCovered) {
        uncoveredCells.add(TableCell.create(column, ""));
      }
    }
    return uncoveredCells;
  }

  private Map<String, Object> buildInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryContent", entryContent);
    information.put("entryId", entryId);
    return information;
  }
}
