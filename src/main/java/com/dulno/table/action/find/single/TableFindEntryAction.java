package com.dulno.table.action.find.single;

import com.dulno.workflow.action.Action;
import com.dulno.workflow.action.ActionContentDatabaseTable;
import com.dulno.workflow.action.ActionInformation;
import com.dulno.core.database.*;
import com.dulno.workflow.component.input.InputComponentDataType;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentVariable;
import com.dulno.workflow.component.output.ListOutputComponentVariable;
import com.dulno.workflow.component.output.OutputComponentVariable;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFindEntryAction implements Action<TableFindEntryActionExecutor> {
  public static TableFindEntryAction create(
    InputComponentSelect tableComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("column", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("value", DatabaseDataType.TEXT));
    return new TableFindEntryAction(tableComponentSelect, tableDatabaseTable,
      tableFactory, ActionContentDatabaseTable.create(databaseConnection,
      databaseKeyspace, "action_database_entry_find", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-entry-find-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.entry.find.name")
      .withDescription("table.action.entry.find.description")
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.find.input.table.name",
        "tableIdentifier", "table.action.entry.find.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entry.find.input.column.name",
        "entryColumn", "table.action.entry.find.input.column.description", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entry.find.input.value.name",
        "entryValue", "table.action.entry.find.input.value.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.find.output.entry.id", "entryId"))
      .withOutputVariable(ListOutputComponentVariable.create("table.action.entry.find.output.entry.cells", "entryCells",
        OutputComponentVariable.create("table.action.entry.find.output.entry.cell.column", "entryCellColumn"),
        OutputComponentVariable.create("table.action.entry.find.output.entry.cell.value", "entryCellValue")))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.find.output.entry.cells.number", "entryCellsNumber"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.find.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.find.output.column", "entryColumn"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.find.output.value", "entryValue"))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
  }

  @Override
  public CompletableFuture<Void> insert(UUID actionId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(
      content.get("tableIdentifier"), content.get("entryColumn"),
      content.get("entryValue")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(1).stringValue(),
        "entryColumn", row.findCell(2).stringValue(),
        "entryValue", row.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<TableFindEntryActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableFindEntryActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).stringValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
