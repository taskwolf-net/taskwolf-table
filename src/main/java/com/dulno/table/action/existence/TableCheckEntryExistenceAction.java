package com.dulno.table.action.existence;

import com.dulno.core.action.Action;
import com.dulno.core.action.ActionContentDatabaseTable;
import com.dulno.core.action.ActionInformation;
import com.dulno.core.database.*;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentSelect;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.core.workflow.component.output.OutputComponentVariable;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableCheckEntryExistenceAction implements Action<TableCheckEntryExistenceActionExecutor> {
  public static TableCheckEntryExistenceAction create(
    InputComponentSelect tableComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("column", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("value", DatabaseDataType.TEXT));
    return new TableCheckEntryExistenceAction(tableComponentSelect, tableDatabaseTable,
      tableFactory, ActionContentDatabaseTable.create(databaseConnection,
      databaseKeyspace, "action_database_check_entry_existence", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-check-entry-existence-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.check.entry.existence.name")
      .withDescription("table.action.check.entry.existence.description")
      .withInputVariable(InputComponentVariable.createSelect("table.action.check.entry.existence.input.table.name",
        "tableIdentifier", "table.action.check.entry.existence.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.check.entry.existence.input.column.name",
        "entryColumn", "table.action.check.entry.existence.input.column.description", InputComponentDataType.TEXT))
      .withInputVariable(InputComponentVariable.createRequired("table.action.check.entry.existence.input.value.name",
        "entryValue", "table.action.check.entry.existence.input.value.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.check.entry.existence.output.exists", "entryExists"))
      .withOutputVariable(OutputComponentVariable.create("table.action.check.entry.existence.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.action.check.entry.existence.output.column", "entryColumn"))
      .withOutputVariable(OutputComponentVariable.create("table.action.check.entry.existence.output.value", "entryValue"))
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
  public CompletableFuture<TableCheckEntryExistenceActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableCheckEntryExistenceActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).stringValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
