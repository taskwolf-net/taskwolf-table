package com.dulno.table.select;

import com.dulno.core.locale.Translation;
import com.dulno.core.user.User;
import com.dulno.workflow.component.input.InputComponentSelect;
import com.dulno.workflow.component.input.InputComponentSelectEntry;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "create")
public class TableAggregationComponentSelect implements InputComponentSelect {
  private final Translation translation;

  @Override
  public CompletableFuture<List<InputComponentSelectEntry>> compile(
    User user, UUID target, Map<String, String> previousInputs
  ) {
    return CompletableFuture.completedFuture(Lists.newArrayList(
      InputComponentSelectEntry.create("SUM",
        translation.translate(user, "table.aggregation.sum")),
      InputComponentSelectEntry.create("AVG",
        translation.translate(user, "table.aggregation.avg")),
      InputComponentSelectEntry.create("MIN",
        translation.translate(user, "table.aggregation.min")),
      InputComponentSelectEntry.create("MAX",
        translation.translate(user, "table.aggregation.max"))));
  }
}
