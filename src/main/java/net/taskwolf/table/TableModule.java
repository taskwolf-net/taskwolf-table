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
import net.taskwolf.table.action.TableActionFactory;
import net.taskwolf.table.trigger.TableTriggerFactory;
import org.springframework.boot.SpringApplication;

import java.util.List;

@ModuleDescription(name = "table", version = "1.0.0-SNAPSHOT",
  priority = ModuleLoadPriority.NEUTRAL)
public final class TableModule extends Module {
  private Log log;
  private TriggerFactory triggerFactory;
  private ActionFactory actionFactory;
  private AccountLink accountLink;

  public TableModule(Injector injector) {
    super(injector.createChildInjector(TableInjectionModule.create()));
  }

  @Override
  public void enable() throws Exception {
    log = injector().getInstance(Log.class).subLog("Table");
    injector().getInstance(SpringApplication.class).addInitializers(
      injector().getInstance(TableContextInitializer.class));
    triggerFactory = TableTriggerFactory.create();
    actionFactory = TableActionFactory.create();
    accountLink = TableAccountLink.create();
  }

  @Override
  public void disable() {

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
    return Lists.newArrayList();
  }

  @Override
  public List<ActionInformation> actionInformation() {
    return Lists.newArrayList();
  }
}