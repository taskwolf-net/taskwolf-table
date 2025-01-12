package com.dulno.table.structure;

import com.dulno.core.error.ErrorRepository;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import com.dulno.core.database.DatabaseColumn;
import com.dulno.core.database.DatabaseRow;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class TableRow {
  public static TableRow of(
    ErrorRepository errorRepository, DatabaseRow row, List<DatabaseColumn> columns
  ) {
    var cells = Lists.<TableCell>newArrayList();
    for (var i = 0; i < row.cellNumber(); i++) {
      cells.add(i, TableCell.create(columns.get(i).name(),
        row.findCell(i).value()));
    }
    return create(errorRepository, cells);
  }

  private final ErrorRepository errorRepository;
  private final List<TableCell> cells;

  public long size() {
    long size = 0L;
    for (var cell : cells) {
      var value = cell.value();
      if (value == null) {
        size += 14;
      } else if (value instanceof UUID) {
        size += 16;
      } else if (value instanceof Long) {
        size += 8;
      } else if (value instanceof String string) {
        size += string.getBytes(StandardCharsets.UTF_8).length;
      }
    }
    return size;
  }
}
