package net.taskwolf.table.access;

import net.taskwolf.core.access.TaskwolfRequestBody;
import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.iterator.AsyncIterator;
import net.taskwolf.core.locale.Translation;
import net.taskwolf.core.organization.team.Team;
import net.taskwolf.core.organization.team.TeamDatabaseTable;
import net.taskwolf.core.organization.team.TeamTargetDatabaseTable;
import net.taskwolf.core.user.User;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@RestController
public final class TableCreateController extends TableController {
  private final TableFactory tableFactory;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final Translation translation;

  private TableCreateController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable,
    TableFactory tableFactory, TableColumnDatabaseTable tableColumnDatabaseTable,
    BundleDatabaseTable bundleDatabaseTable, TeamDatabaseTable teamDatabaseTable,
    Translation translation
  ) {
    super(secretKey, userDatabaseTable, tableDatabaseTable,
      userTargetDatabaseTable, teamTargetDatabaseTable);
    this.tableFactory = tableFactory;
    this.tableColumnDatabaseTable = tableColumnDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.translation = translation;
  }

  @RequestMapping(path = "/table/create/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> createTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var name = body.getSanitizedString("name", 64);
    if (name.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return findUser(request)
      .thenCompose(user -> userTargetDatabaseTable().findTargetSecured(user.id())
        .thenCompose(target -> findTableOwner(user, target)
          .thenCompose(owner -> tableDatabaseTable().generateAvailableTableId()
            .thenCompose(tableId -> tableColumnDatabaseTable.generateAvailableColumnId()
              .thenCompose(columnId -> checkDatabaseNumberLimit(user, target)
                .thenCompose(limitReached -> createTable(user, tableId, columnId,
                  owner, name, limitReached, response)))))));
  }

  private CompletableFuture<Map<String, Object>> createTable(
    User user, String tableId, String columnId, UUID owner,
    String name, boolean limitReached, HttpServletResponse response
  ) {
    if (limitReached) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return CompletableFuture.completedFuture(Maps.newHashMap());
    }
    var processes = Lists.<CompletableFuture<Void>>newArrayList();
    var entry = TableEntry.create(owner, tableId, user.id(), name,
      Lists.newArrayList(columnId), System.currentTimeMillis(), 0);
    processes.add(tableDatabaseTable().insertTable(entry));
    var tableColumn = TableColumn.create(columnId, tableId, TableColumnType.TEXT,
      translation.translate(user, "table.column.default"));
    processes.add(tableColumnDatabaseTable.insertColumn(tableColumn));
    var table = tableFactory.create(entry, Lists.newArrayList(tableColumn));
    processes.add(table.createAsyncIfNotExists()
      .thenCompose(value -> table.createIndexAsyncIfNotExists("id"))
      .thenCompose(value -> table.createIndexAsyncIfNotExists("timestamp"))
      .thenCompose(value -> table.createIndexAsyncIfNotExists(columnId)));
    return AsyncIterator.execute(processes, process -> process)
      .thenApply(value -> Map.of("table", tableId));
  }

  private CompletableFuture<UUID> findTableOwner(User user, UUID target) {
    return user.id().equals(target) ?
      CompletableFuture.completedFuture(target) :
      teamTargetDatabaseTable().findTargetSecured(user.id())
        .thenApply(team -> team.orElse(target));
  }

  private CompletableFuture<Boolean> checkDatabaseNumberLimit(
    User user, UUID target
  ) {
    return findOwnersOfTarget(user, target)
      .thenCompose(owners -> AsyncIterator.execute(owners, owner ->
          tableDatabaseTable().findTableCount(owner))
        .thenApply(sizes -> sizes.stream().mapToLong(Long::longValue).sum())
        .thenCompose(number -> bundleDatabaseTable.findBundle(target)
          .thenApply(bundle -> bundle.databaseNumberLimit() > 0 &&
            number >= bundle.databaseNumberLimit())));
  }

  private CompletableFuture<List<UUID>> findOwnersOfTarget(User user, UUID target) {
    return user.id().equals(target) ?
      CompletableFuture.completedFuture(Lists.newArrayList(target)) :
      teamDatabaseTable.findTeamsByOrganization(target).thenApply(teams ->
        Stream.concat(teams.stream().map(Team::id).toList().stream(),
          Stream.of(target)).toList());
  }
}