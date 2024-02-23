package net.taskwolf.table.action;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import lombok.AllArgsConstructor;
import net.taskwolf.core.action.Action;
import net.taskwolf.core.action.ActionInformation;
import net.taskwolf.core.action.ActionResult;
import net.taskwolf.core.workflow.component.input.InputComponentDataType;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
import net.taskwolf.core.workflow.component.input.InputComponentVariable;
import net.taskwolf.core.workflow.component.output.OutputComponentVariable;
import net.taskwolf.core.workflow.placeholder.PlaceholderDissolve;
import net.taskwolf.table.structure.Table;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableInsertEntryAction implements Action {
  public static ActionInformation information(
    InputComponentSelect tableComponentSelect
  ) {
    return ActionInformation.builder()
      .withName("table.action.entry.insert.name")
      .withDescription("table.action.entry.insert.description")
      .withIdentifier("table-entry-insert-action")
      .withInputVariable(InputComponentVariable.createSelect("table.action.entry.insert.input.table.name",
        "tableIdentifier", "table.action.entry.insert.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createRequired("table.action.entry.insert.input.content.name",
        "entryContent", "table.action.entry.insert.input.content.description", InputComponentDataType.TEXT))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.action.entry.insert.output.entry", "entry"))
      .build();
  }

  public static TableInsertEntryAction of(
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    JSONObject content
  ) {
    return create(tableDatabaseTable, tableFactory,
      content.getString("tableIdentifier"), content.getString("entryContent"));
  }

  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final String tableIdentifier;
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
      return ActionResult.futureFailure("table.action.entry.insert.failure.table.not.found");
    }
    return tableFactory.create(tableIdentifier).thenApply(table ->
      execute(information, table));
  }

  private ActionResult execute(
    Map<String, Object> information, Table table
  ) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryContent = placeholderDissolve.dissolve(entryContent);
    return ActionResult.success(buildInformation(table));
  }

  private Map<String, Object> buildInformation(Table table) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entry", entryContent);
    return information;
  }
}
