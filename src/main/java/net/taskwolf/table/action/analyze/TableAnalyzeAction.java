package net.taskwolf.table.action.analyze;

import net.taskwolf.core.database.*;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import net.taskwolf.workflow.action.Action;
import net.taskwolf.workflow.action.ActionContentDatabaseTable;
import net.taskwolf.workflow.action.ActionInformation;
import net.taskwolf.workflow.component.ComponentNovelty;
import net.taskwolf.workflow.component.input.InputComponentSelect;
import net.taskwolf.workflow.component.input.InputComponentVariable;
import net.taskwolf.workflow.component.output.OutputComponentVariable;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableAnalyzeAction implements Action<TableAnalyzeActionExecutor> {
  public static TableAnalyzeAction create(
    InputComponentSelect tableComponentSelect,
    InputComponentSelect tableColumnComponentSelect,
    InputComponentSelect tableAggregationComponentSelect,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    DatabaseConnection databaseConnection, DatabaseKeyspace databaseKeyspace
  ) {
    var contentColumns = Lists.<DatabaseColumn>newArrayList();
    contentColumns.add(DatabaseColumn.create("ownerId", DatabaseDataType.UUID));
    contentColumns.add(DatabaseColumn.create("tableId", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("column", DatabaseDataType.TEXT));
    contentColumns.add(DatabaseColumn.create("aggregation", DatabaseDataType.TEXT));
    return new TableAnalyzeAction(tableComponentSelect, tableColumnComponentSelect,
      tableAggregationComponentSelect, tableDatabaseTable, tableFactory,
      ActionContentDatabaseTable.create(databaseConnection, databaseKeyspace,
        "action_database_analyze", contentColumns));
  }

  private final InputComponentSelect tableComponentSelect;
  private final InputComponentSelect tableColumnComponentSelect;
  private final InputComponentSelect tableAggregationComponentSelect;
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final ActionContentDatabaseTable contentDatabaseTable;

  @Override
  public String type() {
    return "database-analyze-action";
  }

  @Override
  public ActionInformation information() {
    return ActionInformation.builder()
      .withName("table.action.analyze.name")
      .withDescription("table.action.analyze.description")
      .withInputVariable(InputComponentVariable.createSelect("table.action.analyze.input.table.name",
        "tableIdentifier", "table.action.analyze.input.table.description", tableComponentSelect))
      .withInputVariable(InputComponentVariable.createSelect("table.action.analyze.input.column.name",
        "analysisColumn", "table.action.analyze.input.column.description", tableColumnComponentSelect))
      .withInputVariable(InputComponentVariable.createSelect("table.action.analyze.input.aggregation.name",
        "analysisAggregation", "table.action.analyze.input.aggregation.description", tableAggregationComponentSelect))
      .withOutputVariable(OutputComponentVariable.create("table.action.analyze.output.result", "analysisResult"))
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
      content.get("tableIdentifier"), content.get("analysisColumn"),
      content.get("analysisAggregation")));
  }

  @Override
  public CompletableFuture<Map<String, Object>> findContent(UUID triggerId) {
    return contentDatabaseTable.findContent(triggerId).thenApply(row ->
      Map.of("tableIdentifier", row.findCell(2).stringValue(),
        "analysisColumn", row.findCell(3).stringValue(),
        "analysisAggregation", row.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<TableAnalyzeActionExecutor> build(UUID actionId) {
    return contentDatabaseTable.findContent(actionId).thenApply(content ->
      TableAnalyzeActionExecutor.create(tableDatabaseTable, tableFactory,
        content.findCell(1).uuidValue(), content.findCell(2).stringValue(),
        content.findCell(3).stringValue(), content.findCell(4).stringValue()));
  }

  @Override
  public CompletableFuture<Void> delete(UUID actionId) {
    return contentDatabaseTable.deleteContent(actionId);
  }
}