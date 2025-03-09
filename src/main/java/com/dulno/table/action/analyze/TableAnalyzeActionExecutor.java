package com.dulno.table.action.analyze;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.database.aggregation.DatabaseAggregation;
import com.dulno.table.structure.*;
import com.dulno.workflow.action.ActionExecutor;
import com.dulno.workflow.action.ActionResult;
import lombok.AllArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableAnalyzeActionExecutor implements ActionExecutor {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final UUID ownerId;
  private final String tableIdentifier;
  private final String analysisColumn;
  private final String analysisAggregation;

  @Override
  public CompletableFuture<ActionResult> execute(Map<String, Object> information) {
    return tableDatabaseTable.tableExists(tableIdentifier).thenCompose(exists ->
      execute(information, exists));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, boolean tableExists
  ) {
    if (!tableExists) {
      return ActionResult.futureFailure("table.action.analyze.failure.table.not.found");
    }
    return tableDatabaseTable.findTable(tableIdentifier)
      .thenCompose(tableEntry -> execute(information, tableEntry));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, TableEntry tableEntry
  ) {
    if (!tableEntry.owner().equals(ownerId)) {
      return ActionResult.futureFailure("table.action.analyze.failure.table.permission");
    }
    return tableFactory.create(tableEntry)
      .thenCompose(table -> execute(information, table));
  }

  private CompletableFuture<ActionResult> execute(
    Map<String, Object> information, Table table
  ) {
    if (table.columns().stream().noneMatch(column -> column.name().equals(analysisColumn))) {
      return ActionResult.futureFailure("table.action.analyze.failure.column.not.found");
    }
    var column = table.tableColumns().stream()
      .filter(tableColumn -> tableColumn.id().equals(analysisColumn)).findFirst().get();
    if (column.type() != TableColumnType.NUMBER) {
      return ActionResult.futureFailure("table.action.analyze.failure.column.type");
    }
    try {
      var aggregation = DatabaseAggregation.valueOf(analysisAggregation);
      return table.aggregateContent(aggregation, analysisColumn)
        .thenApply(result -> ActionResult.success(buildInformation(result)));
    } catch (Exception exception) {
      return ActionResult.futureFailure("table.action.analyze.failure.aggregation");
    }
  }

  private Map<String, Object> buildInformation(BigDecimal result) {
    var information = Maps.<String, Object>newHashMap();
    information.put("analysisResult", result);
    return information;
  }
}
