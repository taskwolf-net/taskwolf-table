package net.taskwolf.table.trigger;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import net.taskwolf.core.trigger.Trigger;
import net.taskwolf.core.trigger.TriggerInformation;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
import net.taskwolf.core.workflow.component.input.InputComponentVariable;
import net.taskwolf.core.workflow.component.output.OutputComponentVariable;
import org.json.JSONObject;

@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(staticName = "create")
public final class TableInsertEntryTrigger implements Trigger {
  public static TriggerInformation information(
    InputComponentSelect tableComponentSelect
  ) {
    return TriggerInformation.builder()
      .withName("table.trigger.entry.insert.name")
      .withDescription("table.trigger.entry.insert.description")
      .withIdentifier("database-entry-insert-trigger")
      .withInputVariable(InputComponentVariable.createSelect("table.trigger.entry.insert.input.table.name",
        "tableIdentifier", "table.trigger.entry.insert.input.table.description", tableComponentSelect))
      .withOutputVariable(OutputComponentVariable.create("table.trigger.entry.insert.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.trigger.entry.insert.output.entry.id", "entryId"))
      .build();
  }

  public static TableInsertEntryTrigger of(JSONObject content) {
    return create(content.getString("tableIdentifier"));
  }

  private final String tableIdentifier;
}
