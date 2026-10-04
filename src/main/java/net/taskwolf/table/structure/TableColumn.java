package net.taskwolf.table.structure;

import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseRow;
import net.taskwolf.core.database.DatabaseTable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Accessors(fluent = true)
@AllArgsConstructor(staticName = "create")
public final class TableColumn {
  public static TableColumn of(DatabaseRow row, DatabaseTable table) {
    return of(row, table.columns().stream().map(DatabaseColumn::name).toList());
  }

  public static TableColumn of(DatabaseRow row, List<String> columns) {
    return create(row.findCell(columns.indexOf("id")).stringValue(),
      row.findCell(columns.indexOf("tableId")).stringValue(),
      TableColumnType.valueOf(row.findCell(columns.indexOf("type")).stringValue()),
      row.findCell(columns.indexOf("name")).stringValue());
  }

  private final String id;
  private final String tableId;
  private final TableColumnType type;
  private String name;

  public void changeName(String newName) {
    name = newName;
  }

  public DatabaseColumn toDatabaseColumn() {
    return DatabaseColumn.create(id, type.dataType());
  }
}