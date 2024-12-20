package com.dulno.table.action.find.multiple;

import com.dulno.core.action.Action;
import com.dulno.core.action.ActionContentDatabaseTable;
import com.dulno.core.action.ActionInformation;
import com.dulno.core.database.*;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentSelect;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.core.workflow.component.output.OutputComponentVariable;
import com.dulno.core.workflow.component.output.ListOutputComponentVariable;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFindEntriesAction implements Action<TableFindEntriesActionExecutor> {
  public static TableFindEntriesAction create(
    InputComponentSelect tableComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("column", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("value", DatabaseDataType.TEXT));
    return new TableFindEntriesAction(tableComponentSelect, tableDatabaseTable,
      tableFactory, ActionContentDatabaseTable.create(databaseConnection,
      databaseKeyspace, "action_database_entries_find", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-entries-find-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.entries.find.name")
      .withDescription("table.action.entries.find.description")
      .withInputVariable(InputComponentVariable.createSelect("table.action.entries.find.input.table.name",
        "tableIdentifier", "table.action.entries.find.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entries.find.input.column.name",
        "entriesColumn", "table.action.entries.find.input.column.description", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entries.find.input.value.name",
        "entriesValue", "table.action.entries.find.input.value.description", InputComponentDataType.TEXT))
      .withOutputVariable(ListOutputComponentVariable.create("table.action.entries.find.output.entries", "entries",
          OutputComponentVariable.create("table.action.entries.find.output.entry.id", "entryId"),
          OutputComponentVariable.create("table.action.entries.find.output.entry.content", "entryContent")))
      .withOutputVariable(OutputComponentVariable.create("table.action.entries.find.output.entries.number", "entriesNumber"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entries.find.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entries.find.output.column", "entriesColumn"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entries.find.output.value", "entriesValue"))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
  }

  @Override
  public CompletableFuture<Void> insert(UUID actionId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(
      content.get("tableIdentifier"), content.get("entriesColumn"),
      content.get("entriesValue")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(1).stringValue(),
        "entriesColumn", row.findCell(2).stringValue(),
        "entriesValue", row.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<TableFindEntriesActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableFindEntriesActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).stringValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
