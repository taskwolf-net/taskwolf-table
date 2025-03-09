package com.dulno.table.action.column;

import com.dulno.core.user.User;
import com.dulno.table.structure.*;
import com.dulno.workflow.component.input.DynamicInputComponentVariableFunction;
import com.dulno.workflow.component.input.InputComponentDataType;
import com.dulno.workflow.component.input.InputComponentVariable;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "create")
public final class TableColumnFunction implements DynamicInputComponentVariableFunction {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;

  @Override
  public CompletableFuture<List<InputComponentVariable>> compile(
    User user, UUID target, JSONObject jsonObject
  ) {
    var tableId = jsonObject.getString("tableIdentifier");
    return tableDatabaseTable.tableExists(tableId)
      .thenCompose(exists -> checkTablePermission(tableId, target, exists));
  }

  private CompletableFuture<List<InputComponentVariable>> checkTablePermission(
    String tableId, UUID target, boolean exists
  ) {
    if (!exists) {
      return CompletableFuture.completedFuture(Lists.newArrayList());
    }
    return tableDatabaseTable.findTable(tableId)
      .thenCompose(tableEntry -> findTableColumns(tableEntry, target));
  }

  private CompletableFuture<List<InputComponentVariable>> findTableColumns(
    TableEntry tableEntry, UUID target
    ) {
    if (!tableEntry.owner().equals(target)) {
      return CompletableFuture.completedFuture(Lists.newArrayList());
    }
    return tableColumnDatabaseTable.findTableColumns(tableEntry.id())
      .thenApply(columns -> columns.stream()
        .sorted(Comparator.comparing(column -> tableEntry.columns()
          .indexOf(column.id())))
        .toList())
      .thenApply(this::createInputVariables);
  }

  private List<InputComponentVariable> createInputVariables(
    List<TableColumn> columns
  ) {
    var variables = Lists.<InputComponentVariable>newArrayList();
    for (var column : columns) {
      variables.add(InputComponentVariable.createOptional(column.name(),
        "column_" + column.id(), "", findComponentDataType(column)));
    }
    return variables;
  }

  private InputComponentDataType findComponentDataType(TableColumn column) {
    return switch (column.type()) {
      case TableColumnType.TEXT -> InputComponentDataType.TEXT;
      case TableColumnType.TEXT_AREA -> InputComponentDataType.TEXT_AREA;
      case TableColumnType.NUMBER -> InputComponentDataType.TEXT;
      case TableColumnType.DATE -> InputComponentDataType.DATE;
      case TableColumnType.SWITCH -> InputComponentDataType.BOOLEAN;
      case TableColumnType.CHECKBOX -> InputComponentDataType.BOOLEAN;
    };
  }
}
