package net.taskwolf.table.action;

import lombok.RequiredArgsConstructor;
import net.taskwolf.core.action.Action;
import net.taskwolf.core.action.ActionFactory;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import org.json.JSONObject;

@RequiredArgsConstructor(staticName = "create")
public final class TableActionFactory implements ActionFactory {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;

  @Override
  public Action create(String type, String content) {
    var json = new JSONObject(content);
    if (type.equals("table-entry-insert-action")) {
      return TableInsertEntryAction.of(tableDatabaseTable, tableFactory, json);
    }
    if (type.equals("table-entry-remove-action")) {
      return TableRemoveEntryAction.of(tableDatabaseTable, tableFactory, json);
    }
    return null;
  }
}
