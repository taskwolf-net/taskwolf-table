package com.dulno.table;

import com.dulno.core.error.ErrorRepository;
import com.dulno.table.action.existence.TableCheckEntryExistenceAction;
import com.dulno.table.action.find.multiple.TableFindEntriesAction;
import com.dulno.table.action.find.single.TableFindEntryAction;
import com.dulno.table.action.insert.TableInsertEntryAction;
import com.dulno.table.action.remove.TableRemoveEntryAction;
import com.dulno.table.structure.TableColumnDatabaseTable;
import com.dulno.table.structure.TableDatabaseTable;
import com.dulno.table.structure.TableFactory;
import com.dulno.table.trigger.insert.TableInsertEntryTrigger;
import com.dulno.table.trigger.remove.TableRemoveEntryTrigger;
import com.dulno.workflow.integration.Integration;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.dulno.core.account.AccountLink;
import com.dulno.workflow.action.ActionRepository;
import com.dulno.core.database.DatabaseConnection;
import com.dulno.core.database.DatabaseKeyspace;
import com.dulno.core.log.Log;
import com.dulno.core.module.ModuleDescription;
import com.dulno.core.module.ModuleInformation;
import com.dulno.core.module.ModuleLoadPriority;
import com.dulno.workflow.trigger.TriggerRepository;
import com.dulno.workflow.component.input.InputComponentSelect;
import org.springframework.boot.SpringApplication;

@ModuleDescription(name = "table", version = "1.0.0-SNAPSHOT",
  priority = ModuleLoadPriority.NEUTRAL)
public final class TableModule extends Integration {
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
    var tableColumnDatabaseTable = injector().getInstance(TableColumnDatabaseTable.class);
    var tableFactory = injector().getInstance(TableFactory.class);
    contextInitializer = TableContextInitializer.create(tableDatabaseTable,
      tableColumnDatabaseTable, tableFactory);
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
    var tableDatabaseTable = injector().getInstance(TableDatabaseTable.class);
    var repository = TriggerRepository.create();
    repository.registerTrigger(TableInsertEntryTrigger.create(tableDatabaseTable,
      tableComponentSelect, databaseConnection, databaseKeyspace));
    repository.registerTrigger(TableRemoveEntryTrigger.create(tableDatabaseTable,
      tableComponentSelect, databaseConnection, databaseKeyspace));
    return repository;
  }

  @Override
  public ActionRepository actionRepository() {
    var databaseConnection = injector().getInstance(DatabaseConnection.class);
    var databaseKeyspace = injector().getInstance(DatabaseKeyspace.class);
    var tableDatabaseTable = injector().getInstance(TableDatabaseTable.class);
    var tableFactory = injector().getInstance(TableFactory.class);
    var errorRepository = injector().getInstance(ErrorRepository.class);
    var repository = ActionRepository.create();
    repository.registerAction(TableInsertEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableFactory, errorRepository, databaseConnection,
      databaseKeyspace));
    repository.registerAction(TableRemoveEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableCheckEntryExistenceAction.create(
      tableComponentSelect, tableDatabaseTable, tableFactory, databaseConnection,
      databaseKeyspace));
    repository.registerAction(TableFindEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableFindEntriesAction.create(tableComponentSelect,
      tableDatabaseTable, tableFactory, databaseConnection, databaseKeyspace));
    return repository;
  }
}