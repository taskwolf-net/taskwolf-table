package net.taskwolf.table.action.update;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import net.taskwolf.core.database.DatabaseDataType;
import net.taskwolf.core.error.ErrorRepository;
import net.taskwolf.table.structure.*;
import net.taskwolf.workflow.action.ActionExecutor;
import net.taskwolf.workflow.action.ActionResult;
import net.taskwolf.workflow.placeholder.PlaceholderDissolve;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableUpdateEntryActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ErrorRepository errorRepository;
  private final UUID ownerId;
  private final String tableIdentifier;
  private String entryIdentifier;
  private String entryContent;

  @Override
  public CompletableFuture<ActionResult> execute(Map<String, Object> information) {
    return tableDatabaseTable.tableExists(tableIdentifier).thenCompose(exists ->
      execute(information, exists));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, boolean tableExists
  ) {
    if (!tableExists) {
      return ActionResult.futureFailure("table.action.entry.update.failure.table.not.found");
    }
    return tableDatabaseTable.findTable(tableIdentifier)
      .thenCompose(tableEntry -> execute(information, tableEntry));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry
  ) {
    if (!tableEntry.owner().equals(ownerId)) {
      return ActionResult.futureFailure("table.action.entry.update.failure.table.permission");
    }
    return tableFactory.create(tableEntry)
      .thenCompose(table -> execute(information, tableEntry, table));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry, Table table
  ) {
    var formattedInformation = Maps.<String, Object>newHashMap();
    for (var entry : information.entrySet()) {
      formattedInformation.put(entry.getKey(),
        entry.getValue().toString().replace("\\", "\\\\")
          .replace("\"", "\\\"").replace("\b", "\\b").replace("\f", "\\f")
          .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t"));
    }
    var placeholderDissolve = PlaceholderDissolve.create(formattedInformation);
    entryContent = placeholderDissolve.dissolve(entryContent);
    entryIdentifier = placeholderDissolve.dissolve(entryIdentifier);
    try {
      var entryId = UUID.fromString(entryIdentifier);
      return table.contentExists(entryId).thenCompose(exists ->
        execute(tableEntry, table, entryId, exists));
    } catch (Exception exception) {
      return ActionResult.futureFailure("table.action.entry.update.failure.entry.wrong.format");
    }
  }

  private CompletableFuture<ActionResult> execute(
    TableEntry tableEntry, Table table, UUID entryId, boolean entryExists
  ) {
    if (!entryExists) {
      return ActionResult.futureFailure("table.action.entry.update.failure.entry.not.found");
    }
    var cells = createCells(tableEntry, table, entryId);
    return table.updateContent(entryId, TableRow.create(errorRepository, cells))
      .thenApply(success -> success ? ActionResult.success(Maps.newHashMap()) :
        ActionResult.failure("table.action.entry.update.failure.data.limit.reached"));
  }

  private List<TableCell> createCells(
    TableEntry tableEntry, Table table, UUID entryId
  ) {
    var content = new JSONObject(entryContent);
    var tableColumns = table.tableColumns();
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("owner", tableEntry.owner()));
    cells.add(TableCell.create("timestamp", System.currentTimeMillis()));
    cells.add(TableCell.create("id", entryId));
    for (var column : tableColumns) {
      cells.add(TableCell.create(column.id(), findCellValue(content, column)));
    }
    return cells;
  }

  private Object findCellValue(JSONObject content, TableColumn column) {
    if (!content.has(column.id())) {
      if (column.type().dataType() == DatabaseDataType.BOOLEAN) {
        return false;
      }
      if (column.type().dataType() == DatabaseDataType.DECIMAL) {
        return null;
      }
      return "";
    }
    if (column.type().dataType() == DatabaseDataType.BOOLEAN) {
      return content.getBoolean(column.id());
    }
    if (column.type().dataType() == DatabaseDataType.DECIMAL) {
      return content.getBigDecimal(column.id());
    }
    return content.getString(column.id());
  }
}
