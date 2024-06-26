package net.taskwolf.table.structure;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseRow;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.List;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class TableRow {
  public static TableRow of(DatabaseRow row, List<DatabaseColumn> columns) {
    var cells = Lists.<TableCell>newArrayList();
    for (var i = 0; i < row.cellNumber(); i++) {
      cells.add(i, TableCell.create(columns.get(i).name(),
        row.findCell(i).value()));
    }
    return create(cells);
  }

  private final List<TableCell> cells;

  public long size() {
    var byteOutputStream = new ByteArrayOutputStream();
    try {
      var objectOutputStream = new ObjectOutputStream(byteOutputStream);
      for (var cell : cells) {
        objectOutputStream.writeObject(cell.value());
      }
      objectOutputStream.flush();
      return byteOutputStream.toByteArray().length;
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return -1;
  }
}
