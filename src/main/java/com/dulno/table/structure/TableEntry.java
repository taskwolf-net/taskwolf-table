package com.dulno.table.structure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import com.dulno.core.database.DatabaseColumn;
import com.dulno.core.database.DatabaseRow;
import com.dulno.core.database.DatabaseTable;

import java.util.List;
import java.util.UUID;

@Getter
@Accessors(fluent = true)
@AllArgsConstructor(staticName = "create")
public final class TableEntry {
  public static TableEntry of(DatabaseRow row, DatabaseTable table) {
    return of(row, table.columns().stream().map(DatabaseColumn::name).toList());
  }

  public static TableEntry of(DatabaseRow row, List<String> columns) {
    return create(row.findCell(columns.indexOf("owner")).uuidValue(),
      row.findCell(columns.indexOf("id")).stringValue(),
      row.findCell(columns.indexOf("creator")).uuidValue(),
      row.findCell(columns.indexOf("name")).stringValue(),
      row.findCell(columns.indexOf("columns")).listValue(),
      row.findCell(columns.indexOf("created")).longValue(),
      row.findCell(columns.indexOf("size")).longValue());
  }

  private final UUID owner;
  private final String id;
  private final UUID creator;
  private String name;
  private List<String> columns;
  private final long created;
  private long size;

  public void changeName(String newName) {
    name = newName;
  }

  public void updateColumns(List<String> newColumns) {
    columns = newColumns;
  }

  public void updateSize(long newSize) {
    size = newSize;
  }
}
