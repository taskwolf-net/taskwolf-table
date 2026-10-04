package net.taskwolf.table;

import net.taskwolf.core.account.AccountLink;
import net.taskwolf.core.database.DatabaseConnection;
import net.taskwolf.core.database.DatabaseKeyspace;
import net.taskwolf.core.error.ErrorRepository;
import net.taskwolf.core.locale.Translation;
import net.taskwolf.core.log.Log;
import net.taskwolf.core.module.ModuleDescription;
import net.taskwolf.core.module.ModuleInformation;
import net.taskwolf.core.module.ModuleLoadPriority;
import net.taskwolf.table.action.analyze.TableAnalyzeAction;
import net.taskwolf.table.action.existence.TableCheckEntryExistenceAction;
import net.taskwolf.table.action.find.multiple.TableFindEntriesAction;
import net.taskwolf.table.action.find.single.TableFindEntryAction;
import net.taskwolf.table.action.flip.TableFlipEntryAction;
import net.taskwolf.table.action.insert.TableInsertEntryAction;
import net.taskwolf.table.action.remove.TableRemoveEntryAction;
import net.taskwolf.table.action.update.TableUpdateEntryAction;
import net.taskwolf.table.select.TableAggregationComponentSelect;
import net.taskwolf.table.select.TableColumnComponentSelect;
import net.taskwolf.table.select.TableComponentSelect;
import net.taskwolf.table.structure.TableColumnDatabaseTable;
import net.taskwolf.table.structure.TableColumnType;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import net.taskwolf.table.trigger.insert.TableInsertEntryTrigger;
import net.taskwolf.table.trigger.remove.TableRemoveEntryTrigger;
import net.taskwolf.workflow.action.ActionRepository;
import net.taskwolf.workflow.component.input.InputComponentSelect;
import net.taskwolf.workflow.integration.Integration;
import net.taskwolf.workflow.trigger.TriggerRepository;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.springframework.boot.SpringApplication;

@ModuleDescription(name = "table", version = "1.0.0-SNAPSHOT",
  priority = ModuleLoadPriority.NEUTRAL)
public final class TableModule extends Integration {
  private Log log;
  private SpringApplication springApplication;
  private TableContextInitializer contextInitializer;
  private AccountLink accountLink;
  private InputComponentSelect tableComponentSelect;
  private InputComponentSelect tableColumnComponentSelect;
  private InputComponentSelect numberTableColumnComponentSelect;
  private InputComponentSelect flipTableColumnComponentSelect;
  private InputComponentSelect tableAggregationComponentSelect;

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
    accountLink = TableAccountLink.create(tableDatabaseTable);
    tableComponentSelect = TableComponentSelect.create(tableDatabaseTable);
    tableColumnComponentSelect = TableColumnComponentSelect.create(
      tableDatabaseTable, tableColumnDatabaseTable, Lists.newArrayList());
    numberTableColumnComponentSelect = TableColumnComponentSelect.create(
      tableDatabaseTable, tableColumnDatabaseTable,
      Lists.newArrayList(TableColumnType.NUMBER));
    flipTableColumnComponentSelect = TableColumnComponentSelect.create(
      tableDatabaseTable, tableColumnDatabaseTable,
      Lists.newArrayList(TableColumnType.SWITCH, TableColumnType.CHECKBOX));
    tableAggregationComponentSelect = TableAggregationComponentSelect.create(
      injector().getInstance(Translation.class));
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
    return ModuleInformation.create("table.module", "", "database.png",
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
    var tableColumnDatabaseTable = injector().getInstance(TableColumnDatabaseTable.class);
    var tableFactory = injector().getInstance(TableFactory.class);
    var errorRepository = injector().getInstance(ErrorRepository.class);
    var repository = ActionRepository.create();
    repository.registerAction(TableInsertEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableColumnDatabaseTable, tableFactory, errorRepository,
      databaseConnection, databaseKeyspace));
    repository.registerAction(TableRemoveEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableUpdateEntryAction.create(tableComponentSelect,
      tableDatabaseTable, tableColumnDatabaseTable, tableFactory, errorRepository,
      databaseConnection, databaseKeyspace));
    repository.registerAction(TableCheckEntryExistenceAction.create(
      tableComponentSelect, tableColumnComponentSelect, tableDatabaseTable,
      tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableFindEntryAction.create(tableComponentSelect,
      tableColumnComponentSelect, tableDatabaseTable, tableColumnDatabaseTable,
      tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableFindEntriesAction.create(tableComponentSelect,
      tableColumnComponentSelect, tableDatabaseTable, tableColumnDatabaseTable,
      tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableAnalyzeAction.create(tableComponentSelect,
      numberTableColumnComponentSelect, tableAggregationComponentSelect,
      tableDatabaseTable, tableFactory, databaseConnection, databaseKeyspace));
    repository.registerAction(TableFlipEntryAction.create(tableComponentSelect,
      flipTableColumnComponentSelect, tableDatabaseTable, tableFactory,
      errorRepository, databaseConnection, databaseKeyspace));
    return repository;
  }
}