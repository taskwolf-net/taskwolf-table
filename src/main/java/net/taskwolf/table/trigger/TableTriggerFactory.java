package net.taskwolf.table.trigger;

import lombok.RequiredArgsConstructor;
import net.taskwolf.core.trigger.Trigger;
import net.taskwolf.core.trigger.TriggerFactory;
import org.json.JSONObject;

@RequiredArgsConstructor(staticName = "create")
public class TableTriggerFactory implements TriggerFactory {
  @Override
  public Trigger create(String type, String content) {
    var json = new JSONObject(content);
    if (type.equals("table-entry-insert-trigger")) {
      return TableInsertEntryTrigger.of(json);
    }
    if (type.equals("table-entry-remove-trigger")) {
      return TableRemoveEntryTrigger.of(json);
    }
    return null;
  }
}
