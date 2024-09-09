package net.taskwolf.table;

import lombok.RequiredArgsConstructor;
import net.taskwolf.core.user.User;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
import net.taskwolf.core.workflow.component.input.InputComponentSelectEntry;
import net.taskwolf.table.structure.TableDatabaseTable;
import org.json.JSONObject;

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
