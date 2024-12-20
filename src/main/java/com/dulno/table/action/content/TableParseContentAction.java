package com.dulno.table.action.content;

import com.dulno.core.action.Action;
import com.dulno.core.action.ActionContentDatabaseTable;
import com.dulno.core.action.ActionInformation;
import com.dulno.core.database.*;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.core.workflow.component.output.ListOutputComponentVariable;
import com.dulno.core.workflow.component.output.OutputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableParseContentAction implements Action<TableParseContentActionExecutor> {
  public static TableParseContentAction create(
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("content", DatabaseDataType.TEXT));
    return new TableParseContentAction(
      ActionContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "action_database_content_parse", contentColumns));
  }

  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-content-parse-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.content.parse.name")
      .withDescription("table.action.content.parse.description")
      .withInputVariable(InputComponentVariable.createRequired("table.action.content.parse.input.content.name",
        "entryContent", "table.action.content.parse.input.content.description",
        "table.action.entry.insert.input.content.placeholder", InputComponentDataType.TEXT))
      .withOutputVariable(ListOutputComponentVariable.create("table.action.content.parse.output.cells", "cells",
          OutputComponentVariable.create("table.action.content.parse.output.cell.column", "cellColumn"),
          OutputComponentVariable.create("table.action.content.parse.output.cell.value", "cellValue")))
      .withOutputVariable(OutputComponentVariable.create("table.action.content.parse.output.cells.number", "cellsNumber"))
      .withOutputVariable(OutputComponentVariable.create("table.action.content.parse.output.content", "entryContent"))
      .build();
  }

  @Override
  public void initialize() {
    contentDatabaseTable.createIfNotExists();
  }

  @Override
  public CompletableFuture<Void> insert(UUID actionId, Map<String, Object> content) {
    return contentDatabaseTable.insertContent(actionId, DatabaseRow.of(
      content.get("entryContent")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("entryContent", row.findCell(1).stringValue()));
  }

  @Override
  public CompletableFuture<TableParseContentActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableParseContentActionExecutor.create(content.findCell(1).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}
