package com.dulno.table.action.find.multiple;

import com.dulno.core.database.*;
import com.dulno.table.structure.TableColumnDatabaseTable;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.dulno.workflow.action.Action;
import com.dulno.workflow.action.ActionContentDatabaseTable;
import com.dulno.workflow.action.ActionInformation;
import com.dulno.workflow.component.input.InputComponentDataType;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentVariable;
import com.dulno.workflow.component.output.DynamicOutputComponentVariable;
import com.dulno.workflow.component.output.ListOutputComponentVariable;
import com.dulno.workflow.component.output.OutputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.json.JSONObject;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFindEntriesAction implements Action<TableFindEntriesActionExecutor> {
  public static TableFindEntriesAction create(
    InputComponentSelect tableComponentSelect,
    InputComponentSelect tableColumnComponentSelect,
    TableDatabaseTable tableDatabaseTable,
    TableColumnDatabaseTable tableColumnDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("ownerId", DatabaseDataType.UUID));
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("column", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("value", DatabaseDataType.TEXT));
    return new TableFindEntriesAction(tableComponentSelect, tableColumnComponentSelect,
      tableDatabaseTable, tableColumnDatabaseTable, tableFactory,
      ActionContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "action_database_entries_find", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final InputComponentSelect tableColumnComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
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
      .withInputVariable(InputComponentVariable.createSelect("table.action.entries.find.input.column.name",
        "entriesColumn", "table.action.entries.find.input.column.description", tableColumnComponentSelect))
      .withInputVariable(InputComponentVariable.createOptional("table.action.entries.find.input.value.name",
        "entriesValue", "table.action.entries.find.input.value.description", InputComponentDataType.TEXT))
      .withOutputVariable(ListOutputComponentVariable.create("table.action.entries.find.output.entries", "entries",
        OutputComponentVariable.create("table.action.entries.find.output.entry.id", "database_column_id"),
        DynamicOutputComponentVariable.create(this::buildTableEntryOutputs)))
      .withOutputVariable(OutputComponentVariable.create("table.action.entries.find.output.entries.number", "entriesNumber"))
      .build();
  }

  private CompletableFuture<List<OutputComponentVariable>> buildTableEntryOutputs(
    JSONObject currentContent, List<JSONObject> previousContent
  ) {
    try {
      var loopList = currentContent.getString("loopList");
      if (!loopList.matches("%step\\d+-entries%")) {
        return CompletableFuture.completedFuture(Lists.newArrayList());
      }
      var stepEntryIndex = Integer.parseInt(loopList.replaceAll(
        "%step(\\d+)-entries%", "$1"));
      var content = new JSONObject(previousContent.stream()
        .filter(entry -> entry.getString("kind").equals("action") &&
          entry.getInt("index") == stepEntryIndex)
        .findFirst().get().getString("content"));
      var tableIdentifier = content.getString("tableIdentifier");
      return tableDatabaseTable.tableExists(tableIdentifier)
        .thenCompose(exists -> buildTableEntryOutputs(tableIdentifier, exists));
    } catch (Exception exception) {
      return CompletableFuture.completedFuture(Lists.newArrayList());
    }
  }

  private CompletableFuture<List<OutputComponentVariable>> buildTableEntryOutputs(
    String tableId, boolean exists
  ) {
    if (!exists) {
      return CompletableFuture.completedFuture(Lists.newArrayList());
    }
    return tableDatabaseTable.findTable(tableId)
      .thenCompose(table -> tableColumnDatabaseTable.findTableColumns(tableId)
        .thenApply(columns -> columns.stream()
          .sorted(Comparator.comparing(column -> table.columns().indexOf(column.id())))
          .map(column -> OutputComponentVariable.create(column.name(),
            "database_column_" + column.id()))
          .toList()));
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
  }

  @Override
  public CompletableFuture<Void> insert(
    UUID actionId, UUID ownerId, Map<String, Object> content
  ) {
    var entriesValue = content.get("entriesValue");
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(ownerId,
      content.get("tableIdentifier"), content.get("entriesColumn"),
      entriesValue == null ? "" : entriesValue));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(2).stringValue(),
        "entriesColumn", row.findCell(3).stringValue(),
        "entriesValue", row.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<TableFindEntriesActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableFindEntriesActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).uuidValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue(), content.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
