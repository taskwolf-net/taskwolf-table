package net.taskwolf.table.structure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import net.taskwolf.core.database.DatabaseRow;

import java.util.UUID;

@Getter
@Accessors(fluent = true)
@AllArgsConstructor(staticName = "create")
public final class TableEntry {
  public static TableEntry of(DatabaseRow row) {
    return create(row.findCell(0).stringValue(), row.findCell(1).uuidValue(),
      row.findCell(2).uuidValue(), row.findCell(3).stringValue(),
      row.findCell(4).longValue(), row.findCell(5).longValue());
  }

  private final String id;
  private final UUID owner;
  private final UUID creator;
  private String name;
  private final long created;
  private long size;

  public void changeName(String newName) {
    name = newName;
  }

  public void updateSize(long newSize) {
    size = newSize;
  }
}
