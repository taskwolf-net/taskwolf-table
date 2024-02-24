package net.taskwolf.table;

import com.google.common.collect.Lists;
import com.google.inject.Injector;
import net.taskwolf.core.account.AccountLink;
import net.taskwolf.core.action.ActionFactory;
import net.taskwolf.core.action.ActionInformation;
import net.taskwolf.core.log.Log;
import net.taskwolf.core.module.Module;
import net.taskwolf.core.module.ModuleDescription;
import net.taskwolf.core.module.ModuleInformation;
import net.taskwolf.core.module.ModuleLoadPriority;
import net.taskwolf.core.trigger.TriggerFactory;
import net.taskwolf.core.trigger.TriggerInformation;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
import net.taskwolf.table.action.TableActionFactory;
import net.taskwolf.table.action.TableInsertEntryAction;
import net.taskwolf.table.action.TableRemoveEntryAction;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import net.taskwolf.table.trigger.TableInsertEntryTrigger;
import net.taskwolf.table.trigger.TableRemoveEntryTrigger;
import net.taskwolf.table.trigger.TableTriggerFactory;
import org.springframework.boot.SpringApplication;

import java.util.List;

@ModuleDescription(name = "table", version = "1.0.0-SNAPSHOT",
  priority = ModuleLoadPriority.NEUTRAL)
public final class TableModule extends Module {
  private Log log;
  private SpringApplication springApplication;
  private TableContextInitializer contextInitializer;
  private TriggerFactory triggerFactory;
  private ActionFactory actionFactory;
  private AccountLink accountLink;
  private InputComponentSelect tableComponentSelect;

  public TableModule(Injector injector) {
    super(injector.createChildInjector(TableInjectionModule.create()));
  }

  @Override
  public void enable() throws Exception {
    log = injector().getInstance(Log.class).subLog("Table");
    springApplication = injector().getInstance(SpringApplication.class);
    var tableDatabaseTable = injector().getInstance(TableDatabaseTable.class);
    var tableFactory = injector().getInstance(TableFactory.class);
    triggerFactory = TableTriggerFactory.create();
    contextInitializer = TableContextInitializer.create(tableDatabaseTable,
      tableFactory, triggerFactory);
    springApplication.addInitializers(contextInitializer);
    actionFactory = TableActionFactory.create(tableDatabaseTable, tableFactory);
    accountLink = TableAccountLink.create();
    tableComponentSelect = TableComponentSelect.create(tableDatabaseTable);
  }

  @Override
  public void disable() {
    var initializers = Lists.newArrayList(springApplication.getInitializers());
    initializers.remove(contextInitializer);
    springApplication.setInitializers(initializers);
  }

  @Override
  public TriggerFactory triggerFactory() {
    return triggerFactory;
  }

  @Override
  public ActionFactory actionFactory() {
    return actionFactory;
  }

  @Override
  public AccountLink accountLink() {
    return accountLink;
  }

  @Override
  public ModuleInformation moduleInformation() {
    return ModuleInformation.create("Database", "", "database.png",
      ModuleInformation.Type.PUBLIC);
  }

  @Override
  public List<TriggerInformation> triggerInformation() {
    return Lists.newArrayList(TableInsertEntryTrigger.information(tableComponentSelect),
      TableRemoveEntryTrigger.information(tableComponentSelect));
  }

  @Override
  public List<ActionInformation> actionInformation() {
    return Lists.newArrayList(TableInsertEntryAction.information(tableComponentSelect),
      TableRemoveEntryAction.information(tableComponentSelect));
  }
}