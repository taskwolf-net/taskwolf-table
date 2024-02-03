package net.taskwolf.table.action;

import lombok.RequiredArgsConstructor;
import net.taskwolf.core.action.Action;
import net.taskwolf.core.action.ActionFactory;
import org.json.JSONObject;

@RequiredArgsConstructor(staticName = "create")
public final class TableActionFactory implements ActionFactory {
  @Override
  public Action create(String type, String content) {
    var json = new JSONObject(content);
    return null;
  }
}
