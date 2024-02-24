package net.taskwolf.table.action;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import net.taskwolf.core.action.Action;
import net.taskwolf.core.action.ActionInformation;
import net.taskwolf.core.action.ActionResult;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.workflow.component.input.InputComponentDataType;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
import net.taskwolf.core.workflow.component.input.InputComponentVariable;
import net.taskwolf.core.workflow.component.output.OutputComponentVariable;
import net.taskwolf.core.workflow.placeholder.PlaceholderDissolve;
import net.taskwolf.table.structure.*;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableInsertEntryAction implements Action {
  public static ActionInformation information(
    InputComponentSelect tableComponentSelect
  ) {
    return ActionInformation.builder()
      .withName("table.action.entry.insert.name")
      .withDescription("table.action.entry.insert.description")
      .withIdentifier("database-entry-insert-action")
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.insert.input.table.name",
        "tableIdentifier", "table.action.entry.insert.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entry.insert.input.content.name",
        "entryContent", "table.action.entry.insert.input.content.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.entry.content", "entryContent"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.entry.id", "entryId"))
      .build();
  }

  public static TableInsertEntryAction of(
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    JSONObject content
  ) {
    return create(tableDatabaseTable, tableFactory,
      content.getString("tableIdentifier"), content.getString("entryContent"));
  }

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
    return tableFactory.create(tableIdentifier).thenCompose(table ->
      table.generateAvailableContentId().thenApply(contentId ->
        execute(information, table, contentId)));
  }

  private ActionResult execute(
    Map<String, Object> information, Table table, UUID contentId
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryContent = placeholderDissolve.dissolve(entryContent);
    try {
      var cells = createCells(table, contentId);
      table.insertContent(TableRow.create(cells));
      return ActionResult.success(buildInformation(table, contentId));
    } catch (Exception exception) {
      return ActionResult.failure(exception.getMessage());
    }
  }

  private List<TableCell> createCells(Table table, UUID contentId) throws Exception {
    var tableColumns = table.columns().stream().map(DatabaseColumn::name).toList();
    entryContent = entryContent.replace(" ", "");
    var entries = entryContent.split(",");
    var cells = Lists.newArrayList(TableCell.create("id", contentId));
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
    var column = split[0];
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
