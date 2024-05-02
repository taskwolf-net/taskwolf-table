package net.taskwolf.table.action.insert;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import net.taskwolf.core.action.Action;
import net.taskwolf.core.action.ActionContentDatabaseTable;
import net.taskwolf.core.action.ActionInformation;
import net.taskwolf.core.database.*;
import net.taskwolf.core.workflow.component.input.InputComponentDataType;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
import net.taskwolf.core.workflow.component.input.InputComponentVariable;
import net.taskwolf.core.workflow.component.output.OutputComponentVariable;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableInsertEntryAction implements Action<TableInsertEntryActionExecutor> {
  public static TableInsertEntryAction create(
    InputComponentSelect tableComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("table", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("content", DatabaseDataType.TEXT));
    return new TableInsertEntryAction(tableComponentSelect, tableDatabaseTable,
      tableFactory, ActionContentDatabaseTable.create(databaseConnection,
      databaseKeyspace, "action_database_entry_insert", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-entry-insert-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.entry.insert.name")
      .withDescription("table.action.entry.insert.description")
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.insert.input.table.name",
        "tableIdentifier", "table.action.entry.insert.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entry.insert.input.content.name",
        "entryContent", "table.action.entry.insert.input.content.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.entry.content", "entryContent"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.entry.id", "entryId"))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
  }

  @Override
  public CompletableFuture<Void> insert(UUID actionId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(
      content.get("tableIdentifier"), content.get("entryContent")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(1).stringValue(),
        "entryContent", row.findCell(2).stringValue()));
  }

  @Override
  public CompletableFuture<TableInsertEntryActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableInsertEntryActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).stringValue(), content.findCell(2).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
