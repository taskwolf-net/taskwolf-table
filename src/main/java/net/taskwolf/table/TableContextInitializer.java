package net.taskwolf.table;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

@Singleton
@RequiredArgsConstructor(access = AccessLevel.PRIVATE, onConstructor = @__({@Inject}))
public final class TableContextInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    var beanFactory = applicationContext.getBeanFactory();
    beanFactory.registerSingleton("tableDatabaseTable", tableDatabaseTable);
    beanFactory.registerSingleton("tableFactory", tableFactory);
  }
}
