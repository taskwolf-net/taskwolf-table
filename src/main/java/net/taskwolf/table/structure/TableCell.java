package net.taskwolf.table.structure;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class TableCell {
  private final String column;
  private final Object value;
}
