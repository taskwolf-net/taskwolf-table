package com.dulno.table.action.content;

import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.dulno.core.action.ActionExecutor;
import com.dulno.core.action.ActionResult;
import com.dulno.core.workflow.placeholder.PlaceholderDissolve;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.json.JSONArray;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@AllArgsConstructor(staticName = "create")
public final class TableParseContentActionExecutor implements ActionExecutor {
  private String entryContent;

  @Override
  public CompletableFuture<ActionResult> execute(Map<String, Object> information) {
    var placeholderDissolve = PlaceholderDissolve.create(information);
    entryContent = placeholderDissolve.dissolve(entryContent);
    try {
      return ActionResult.futureSuccess(buildInformation(createCells()));
    } catch (Exception exception) {
      return ActionResult.futureFailure(exception.getMessage());
    }
  }

  private List<Map<String, Object>> createCells() throws Exception {
    var entries = entryContent.split(",");
    var cells = Lists.<Map<String, Object>>newArrayList();
    for (var entry : entries) {
      cells.add(createCell(entry));
    }
    return cells;
  }

  private Map<String, Object> createCell(String entry) throws Exception {
    if (!entry.contains("=")) {
      throw new Exception("table.action.content.parse.failure.wrong.schema");
    }
    var split = entry.split("=");
    if (split.length != 2) {
      throw new Exception("table.action.content.parse.failure.wrong.schema");
    }
    var column = split[0].replace(" ", "");
    var value = split[1];
    return Map.of("cellColumn", column.toLowerCase(), "cellValue", value);
  }

  private Map<String, Object> buildInformation(List<Map<String, Object>> cells) {
    var information = Maps.<String, Object>newHashMap();
    information.put("cells", new JSONArray(cells));
    information.put("cellsNumber", cells.size());
    information.put("entryContent", entryContent);
    return information;
  }
}
