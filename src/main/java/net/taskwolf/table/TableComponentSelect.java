package net.taskwolf.table;

import lombok.RequiredArgsConstructor;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
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
  public CompletableFuture<List<String>> compile(
    UUID id, Map<String, String> previousInputs
  ) {
    return tableDatabaseTable.findTablesOfOwner(id)
      .thenApply(tables -> tables.stream().map(table ->
        new JSONObject(Map.of("identifier", table.id(), "name",
          table.name())).toString()).collect(Collectors.toList()));
  }
}
