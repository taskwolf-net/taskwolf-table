package net.taskwolf.table.action.find.single;

import net.taskwolf.core.database.*;
import net.taskwolf.table.structure.TableColumnDatabaseTable;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import net.taskwolf.workflow.action.Action;
import net.taskwolf.workflow.action.ActionContentDatabaseTable;
import net.taskwolf.workflow.action.ActionInformation;
import net.taskwolf.workflow.component.input.InputComponentDataType;
import net.taskwolf.workflow.component.input.InputComponentSelect;
import net.taskwolf.workflow.component.input.InputComponentVariable;
import net.taskwolf.workflow.component.output.DynamicOutputComponentVariable;
import net.taskwolf.workflow.component.output.OutputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.json.JSONObject;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFindEntryAction implements Action<TableFindEntryActionExecutor> {
  public static TableFindEntryAction create(
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
    return new TableFindEntryAction(tableComponentSelect, tableColumnComponentSelect,
      tableDatabaseTable, tableColumnDatabaseTable, tableFactory,
      ActionContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "action_database_entry_find", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final InputComponentSelect tableColumnComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
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
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.find.input.column.name",
        "entryColumn", "table.action.entry.find.input.column.description", tableColumnComponentSelect))
      .withInputVariable(InputComponentVariable.createOptional("table.action.entry.find.input.value.name",
        "entryValue", "table.action.entry.find.input.value.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.find.output.entry.id", "database_column_id"))
      .withOutputVariable(DynamicOutputComponentVariable.create(
        (currentContent, previousActions) -> buildTableEntryOutputs(currentContent)))
      .build();
  }

  private CompletableFuture<List<OutputComponentVariable>> buildTableEntryOutputs(
    JSONObject actionContent
  ) {
    try {
      var tableIdentifier = actionContent.getString("tableIdentifier");
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
    var entryValue = content.get("entryValue");
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(ownerId,
      content.get("tableIdentifier"), content.get("entryColumn"),
      entryValue == null ? "" : entryValue));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(2).stringValue(),
        "entryColumn", row.findCell(3).stringValue(),
        "entryValue", row.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<TableFindEntryActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableFindEntryActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).uuidValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue(), content.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
