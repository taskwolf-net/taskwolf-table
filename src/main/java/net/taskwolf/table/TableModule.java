package net.taskwolf.table;

import com.google.common.collect.Lists;
import com.google.inject.Injector;
import net.taskwolf.core.account.AccountLink;
import net.taskwolf.core.action.ActionRepository;
import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;
import net.taskwolf.core.log.Log;
import net.taskwolf.core.module.Module;
import net.taskwolf.core.module.ModuleDescription;
import net.taskwolf.core.module.ModuleInformation;
import net.taskwolf.core.module.ModuleLoadPriority;
import net.taskwolf.core.trigger.TriggerRepository;
import net.taskwolf.core.workflow.component.input.InputComponentSelect;
import net.taskwolf.table.action.insert.TableInsertEntryAction;
import net.taskwolf.table.action.remove.TableRemoveEntryAction;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import net.taskwolf.table.trigger.insert.TableInsertEntryTrigger;
import net.taskwolf.table.trigger.remove.TableRemoveEntryTrigger;
import org.springframework.boot.SpringApplication;

@ModuleDescription(name = "table", version = "1.0.0-SNAPSHOT",
  priority = ModuleLoadPriority.NEUTRAL)
public final class TableModule extends Module {
  private Log log;
  private SpringApplication springApplication;
  private TableContextInitializer contextInitializer;
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
    contextInitializer = TableContextInitializer.create(tableDatabaseTable,
      tableFactory);
    springApplication.addInitializers(contextInitializer);
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
  public AccountLink accountLink() {
    return accountLink;
  }

  @Override
  public ModuleInformation moduleInformation() {
    return ModuleInformation.create("Database", "", "database.png",
      ModuleInformation.Type.PUBLIC);
  }

  @Override
  public TriggerRepository triggerRepository() {
    var databaseConnection = injector().getInstance(DatabaseConnection.class);
    var databaseKeyspace = injector().getInstance(DatabaseKeyspace.class);
    var repository = TriggerRepository.create();
    repository.registerTrigger(TableInsertEntryTrigger.create(tableComponentSelect,
      databaseConnection, databaseKeyspace));
    repository.registerTrigger(TableRemoveEntryTrigger.create(tableComponentSelect,
      databaseConnection, databaseKeyspace));
    return repository;
  }


  @Override
  public ActionRepository actionRepository() {
    var databaseConnection = injector().getInstance(DatabaseConnection.class);
    var databaseKeyspace = injector().getInstance(DatabaseKeyspace.class);
    var tableDatabaseTable = injector().getInstance(TableDatabaseTable.class);
    var tableFactory = injector().getInstance(TableFactory.class);
    var repository = ActionRepository.create();
    repository.registerAction(TableInsertEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableRemoveEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableFactory, databaseConnection, databaseKeyspace));
    return repository;
  }
}