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
      row.findCell(4).longValue());
  }

  private final String id;
  private final UUID owner;
  private final UUID creator;
  private String name;
  private final long created;

  public void changeName(String newName) {
    name = newName;
  }
}
