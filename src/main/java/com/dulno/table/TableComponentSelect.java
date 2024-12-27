package com.dulno.table;

import com.dulno.table.structure.TableDatabaseTable;
import lombok.RequiredArgsConstructor;
import com.dulno.core.user.User;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentSelectEntry;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RequiredArgsConstructor(staticName = "create")
public class TableComponentSelect implements InputComponentSelect {
  private final TableDatabaseTable tableDatabaseTable;

  @Override
  public CompletableFuture<List<InputComponentSelectEntry>> compile(
    User user, UUID target, Map<String, String> previousInputs
  ) {
    return tableDatabaseTable.findAllTablesOfOwner(target)
      .thenApply(tables -> tables.stream()
        .map(table -> InputComponentSelectEntry.create(table.id(), table.name()))
        .collect(Collectors.toList()));
  }
}
