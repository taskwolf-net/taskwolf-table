package com.dulno.table.action.insert;

import com.dulno.core.database.DatabaseColumn;
import com.dulno.core.user.User;
import com.dulno.core.workflow.component.input.DynamicInputComponentVariableFunction;
import com.dulno.core.workflow.component.input.InputComponentDataType;
import com.dulno.core.workflow.component.input.InputComponentVariable;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableEntry;
import com.dulno.table.structure.TableFactory;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "create")
public final class TableInsertEntryActionColumnFunction
  implements DynamicInputComponentVariableFunction
{
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;

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
    return tableFactory.create(tableEntry)
      .thenApply(table -> createInputVariables(table.columns()));
  }

  private List<InputComponentVariable> createInputVariables(
    List<DatabaseColumn> columns
  ) {
    var variables = Lists.<InputComponentVariable>newArrayList();
    for (var column : columns) {
      if (column.name().equals("id") || column.name().equals("owner")) {
        continue;
      }
      variables.add(InputComponentVariable.createOptional(column.name(),
        "column_" + column.name(), "", InputComponentDataType.TEXT));
    }
    return variables;
  }
}
