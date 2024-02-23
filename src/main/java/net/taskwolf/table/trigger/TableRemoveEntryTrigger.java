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
public final class TableRemoveEntryTrigger implements Trigger {
  public static TriggerInformation information(
    InputComponentSelect tableComponentSelect
  ) {
    return TriggerInformation.builder()
      .withName("table.trigger.entry.remove.name")
      .withDescription("table.trigger.entry.remove.description")
      .withIdentifier("table-entry-remove-trigger")
      .withInputVariable(InputComponentVariable.createSelect("table.trigger.entry.remove.input.table.name",
        "tableIdentifier", "table.trigger.entry.remove.input.table.description", tableComponentSelect))
      .withOutputVariable(OutputComponentVariable.create("table.trigger.entry.remove.output.table", "tableName"))
      .withOutputVariable(OutputComponentVariable.create("table.trigger.entry.remove.output.entry", "entry"))
      .build();
  }

  public static TableRemoveEntryTrigger of(JSONObject content) {
    return create(content.getString("tableIdentifier"));
  }

  private final String tableIdentifier;
}
