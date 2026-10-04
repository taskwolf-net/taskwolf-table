package net.taskwolf.table.action.existence;

import net.taskwolf.core.database.*;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import net.taskwolf.workflow.action.Action;
import net.taskwolf.workflow.action.ActionContentDatabaseTable;
import net.taskwolf.workflow.action.ActionInformation;
import net.taskwolf.workflow.component.input.InputComponentDataType;
import net.taskwolf.workflow.component.input.InputComponentSelect;
import net.taskwolf.workflow.component.input.InputComponentVariable;
import net.taskwolf.workflow.component.output.OutputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableCheckEntryExistenceAction implements Action<TableCheckEntryExistenceActionExecutor> {
  public static TableCheckEntryExistenceAction create(
    InputComponentSelect tableComponentSelect,
    InputComponentSelect tableColumnComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("ownerId", DatabaseDataType.UUID));
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("column", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("value", DatabaseDataType.TEXT));
    return new TableCheckEntryExistenceAction(tableComponentSelect,
      tableColumnComponentSelect, tableDatabaseTable, tableFactory,
      ActionContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "action_database_check_entry_existence", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final InputComponentSelect tableColumnComponentSelect;
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
      .withInputVariable(InputComponentVariable.createSelect("table.action.check.entry.existence.input.column.name",
        "entryColumn", "table.action.check.entry.existence.input.column.description", tableColumnComponentSelect))
      .withInputVariable(InputComponentVariable.createOptional("table.action.check.entry.existence.input.value.name",
        "entryValue", "table.action.check.entry.existence.input.value.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.check.entry.existence.output.exists", "entryExists"))
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
  public CompletableFuture<TableCheckEntryExistenceActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableCheckEntryExistenceActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).uuidValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue(), content.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
