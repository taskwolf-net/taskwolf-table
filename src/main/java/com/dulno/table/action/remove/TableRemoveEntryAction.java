package com.dulno.table.action.remove;

import com.dulno.core.database.*;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.dulno.workflow.action.Action;
import com.dulno.workflow.action.ActionContentDatabaseTable;
import com.dulno.workflow.action.ActionInformation;
import com.dulno.workflow.component.input.InputComponentDataType;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentVariable;
import com.dulno.workflow.component.output.OutputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableRemoveEntryAction implements Action<TableRemoveEntryActionExecutor> {
  public static TableRemoveEntryAction create(
    InputComponentSelect tableComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("ownerId", DatabaseDataType.UUID));
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("entry", DatabaseDataType.TEXT));
    return new TableRemoveEntryAction(tableComponentSelect, tableDatabaseTable,
      tableFactory, ActionContentDatabaseTable.create(databaseConnection,
      databaseKeyspace, "action_database_entry_remove", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-entry-remove-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.entry.remove.name")
      .withDescription("table.action.entry.remove.description")
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.remove.input.table.name",
        "tableIdentifier", "table.action.entry.remove.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entry.remove.input.entry.name",
        "entryIdentifier", "table.action.entry.remove.input.entry.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.remove.output.table", "tableName"))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
  }

  @Override
  public CompletableFuture<Void> insert(
    UUID actionId, UUID ownerId, Map<String, Object> content
  ) {
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(ownerId,
      content.get("tableIdentifier"), content.get("entryIdentifier")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(2).stringValue(),
        "entryIdentifier", row.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<TableRemoveEntryActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableRemoveEntryActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).uuidValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
