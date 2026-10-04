package net.taskwolf.table.select;

import net.taskwolf.core.user.User;
import net.taskwolf.table.structure.*;
import net.taskwolf.workflow.component.input.InputComponentSelect;
import net.taskwolf.workflow.component.input.InputComponentSelectEntry;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "create")
public class TableColumnComponentSelect implements InputComponentSelect {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
  private final List<TableColumnType> columnTypes;

  @Override
  public CompletableFuture<List<InputComponentSelectEntry>> compile(
    User user, UUID target, Map<String, String> previousInputs
  ) {
    try {
      var tableId = previousInputs.get("tableIdentifier");
      return tableDatabaseTable.tableExists(tableId)
        .thenCompose(exists -> checkTableExistence(tableId, target, exists));
    } catch (Exception exception) {
      return CompletableFuture.completedFuture(Lists.newArrayList());
    }
  }

  private CompletableFuture<List<InputComponentSelectEntry>> checkTableExistence(
    String tableId, UUID target, boolean tableExists
  ) {
    if (!tableExists) {
      return CompletableFuture.completedFuture(Lists.newArrayList());
    }
    return tableDatabaseTable.findTable(tableId)
      .thenCompose(table -> checkTableAccess(table, target));
  }

  private CompletableFuture<List<InputComponentSelectEntry>> checkTableAccess(
    TableEntry table, UUID target
  ) {
    if (!table.owner().equals(target)) {
      return CompletableFuture.completedFuture(Lists.newArrayList());
    }
    return tableColumnDatabaseTable.findTableColumns(table.id())
      .thenApply(columns -> columns.stream()
        .sorted(Comparator.comparing(column -> table.columns().indexOf(column.id())))
        .toList())
      .thenApply(this::assemblyTableColumns);
  }

  private List<InputComponentSelectEntry> assemblyTableColumns(
    List<TableColumn> columns
  ) {
    var result = Lists.<InputComponentSelectEntry>newArrayList();
    if (columnTypes.isEmpty()) {
      result.add(InputComponentSelectEntry.create("id", "id"));
    }
    for (var column : columns) {
      if (!columnTypes.isEmpty() && !columnTypes.contains(column.type())) {
        continue;
      }
      result.add(InputComponentSelectEntry.create(column.id(), column.name()));
    }
    return result;
  }
}
