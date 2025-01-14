package com.dulno.table.action.insert;

import com.dulno.core.error.ErrorRepository;
import com.dulno.table.structure.TableColumnDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import com.dulno.workflow.action.Action;
import com.dulno.workflow.action.ActionContentDatabaseTable;
import com.dulno.workflow.action.ActionInformation;
import com.dulno.core.database.*;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentVariable;
import com.dulno.workflow.component.input.DynamicInputComponentVariable;
import com.dulno.workflow.component.output.OutputComponentVariable;
import com.dulno.table.structure.TableDatabaseTable;
import org.json.JSONObject;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableInsertEntryAction implements Action<TableInsertEntryActionExecutor> {
  public static TableInsertEntryAction create(
    InputComponentSelect tableComponentSelect,
    TableDatabaseTable tableDatabaseTable,
    TableColumnDatabaseTable tableColumnDatabaseTable, TableFactory tableFactory,
    ErrorRepository errorRepository, DatabaseConnection databaseConnection,
    DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("ownerId", DatabaseDataType.UUID));
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("content", DatabaseDataType.TEXT));
    return new TableInsertEntryAction(tableComponentSelect, tableDatabaseTable,
      tableColumnDatabaseTable, tableFactory, errorRepository,
      ActionContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "action_database_entry_insert", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
  private final TableFactory tableFactory;
  private final ErrorRepository errorRepository;
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
      .withInputVariable(DynamicInputComponentVariable.create("entryContent",
        Lists.newArrayList("tableIdentifier"),
        TableInsertEntryActionColumnFunction.create(tableDatabaseTable, tableColumnDatabaseTable)))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.entry.id", "entryId"))
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
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(ownerId)
      .concat(encodeContent(content)));
  }

  private DatabaseRow encodeContent(Map<String, Object> content) {
    var entryContent = new JSONObject();
    for (var entry : content.entrySet()) {
      if (entry.getKey().contains("column_")) {
        entryContent.put(entry.getKey().replace("column_", ""), entry.getValue());
      }
    }
    return DatabaseRow.of(content.get("tableIdentifier"), entryContent.toString());
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId)
      .thenApply(this::decodeContent);
  }

  private Map<String, Object> decodeContent(DatabaseRow row) {
    var content = Maps.<String, Object>newHashMap();
    content.put("tableIdentifier", row.findCell(2).stringValue());
    var entryContent = new JSONObject(row.findCell(3).stringValue());
    for (var entry : entryContent.keySet()) {
      content.put("column_" + entry, entryContent.getString(entry));
    }
    return content;
  }

  @Override
  public CompletableFuture<TableInsertEntryActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableInsertEntryActionExecutor.create(tableDatabaseTable, tableFactory,
        errorRepository, content.findCell(1).uuidValue(),
        content.findCell(2).stringValue(), content.findCell(3).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
