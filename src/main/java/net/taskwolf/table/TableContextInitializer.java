package net.taskwolf.table;

import net.taskwolf.table.structure.TableColumnDatabaseTable;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import com.google.inject.Singleton;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

@Singleton
@RequiredArgsConstructor(staticName = "create")
public final class TableContextInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
  private final TableFactory tableFactory;

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    var beanFactory = applicationContext.getBeanFactory();
    beanFactory.registerSingleton("tableDatabaseTable", tableDatabaseTable);
    beanFactory.registerSingleton("tableColumnDatabaseTable",
      tableColumnDatabaseTable);
    beanFactory.registerSingleton("tableFactory", tableFactory);
  }
}
