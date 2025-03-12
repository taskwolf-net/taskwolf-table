package com.dulno.table.action.flip;

import com.dulno.core.database.*;
import com.dulno.core.error.ErrorRepository;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.dulno.workflow.action.Action;
import com.dulno.workflow.action.ActionContentDatabaseTable;
import com.dulno.workflow.action.ActionInformation;
import com.dulno.workflow.component.ComponentNovelty;
import com.dulno.workflow.component.input.InputComponentDataType;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableFlipEntryAction implements Action<TableFlipEntryActionExecutor> {
  public static TableFlipEntryAction create(
    InputComponentSelect tableComponentSelect,
    InputComponentSelect tableColumnComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    ErrorRepository errorRepository, DatabaseConnection databaseConnection,
    DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("ownerId", DatabaseDataType.UUID));
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("column", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("entry", DatabaseDataType.TEXT));
    return new TableFlipEntryAction(tableComponentSelect,
      tableColumnComponentSelect, tableDatabaseTable, tableFactory, errorRepository,
      ActionContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "action_database_entry_flip", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final InputComponentSelect tableColumnComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ErrorRepository errorRepository;
  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-entry-flip-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.entry.flip.name")
      .withDescription("table.action.entry.flip.description")
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.flip.input.table.name",
        "tableIdentifier", "table.action.entry.flip.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.flip.input.column.name",
        "flipColumn", "table.action.entry.flip.input.column.description", tableColumnComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entry.flip.input.entry.name",
        "entryIdentifier", "table.action.entry.flip.input.entry.description", InputComponentDataType.TEXT))
      .withNovelty(ComponentNovelty.NEW)
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
      content.get("tableIdentifier"), content.get("flipColumn"),
      content.get("entryIdentifier")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(2).stringValue(),
        "flipColumn", row.findCell(3).stringValue(),
        "entryIdentifier", row.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<TableFlipEntryActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableFlipEntryActionExecutor.create(tableDatabaseTable, tableFactory,
        errorRepository, content.findCell(1).uuidValue(),
        content.findCell(2).stringValue(), content.findCell(3).stringValue(),
        content.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
