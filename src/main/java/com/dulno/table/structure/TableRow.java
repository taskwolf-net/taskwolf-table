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
import java.util.List;

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
    var byteOutputStream = new ByteArrayOutputStream();
    try {
      var objectOutputStream = new ObjectOutputStream(byteOutputStream);
      for (var cell : cells) {
        objectOutputStream.writeObject(cell.value().toString().getBytes());
      }
      objectOutputStream.flush();
      return byteOutputStream.toByteArray().length;
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return -1;
  }
}
