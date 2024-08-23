package net.taskwolf.table.access;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.taskwolf.core.CoreModule;
import net.taskwolf.core.access.TaskwolfRequestBody;
import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseDataType;
import net.taskwolf.core.database.DatabaseTable;
import net.taskwolf.core.iterator.AsyncIterator;
import net.taskwolf.core.organization.team.Team;
import net.taskwolf.core.organization.team.TeamDatabaseTable;
import net.taskwolf.core.organization.team.TeamTargetDatabaseTable;
import net.taskwolf.core.user.User;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.*;
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
public final class TableModificationController extends TableController {
  private final TableFactory tableFactory;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final CoreModule coreModule;

  private TableModificationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable,
    TableFactory tableFactory, BundleDatabaseTable bundleDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, CoreModule coreModule
  ) {
    super(secretKey, userDatabaseTable, tableDatabaseTable,
      userTargetDatabaseTable, teamTargetDatabaseTable);
    this.tableFactory = tableFactory;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.coreModule = coreModule;
  }

  @RequestMapping(path = "/table/create/", method = RequestMethod.POST)
  public CompletableFuture<Void> createTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var name = body.getString("name");
    if (name.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return findUser(request).thenCompose(user ->
      userTargetDatabaseTable().findTarget(user.id()).thenCompose(target ->
        findTableOwner(user, target).thenCompose(owner ->
          tableDatabaseTable().generateAvailableTableId().thenCompose(tableId ->
            checkDatabaseNumberLimit(user, target).thenAccept(limitReached ->
              createTable(tableId, owner, user.id(), name,
                limitReached, response))))));
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

  private void createTable(
    String tableId, UUID owner, UUID creator, String name, boolean limitReached,
    HttpServletResponse response
  ) {
    if (limitReached) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      return;
    }
    var entry = TableEntry.create(owner, tableId, creator, name,
      System.currentTimeMillis(), 0);
    tableDatabaseTable().insertTable(entry);
    var defaultColumns = Lists.newArrayList(DatabaseColumn.create("id",
      DatabaseDataType.UUID, DatabaseColumn.Type.PRIMARY_KEY),
      DatabaseColumn.create("data", DatabaseDataType.TEXT));
    var table = tableFactory.create(entry, defaultColumns);
    table.createIfNotExists();
  }

  @RequestMapping(path = "/table/entry/insert/", method = RequestMethod.POST)
  public CompletableFuture<Void> insertTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var futureResponse = new CompletableFuture<Void>();
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableEntry).thenAccept(table ->
        table.generateAvailableContentId().thenAccept(contentId ->
          insertTableEntry(tableEntry, table, contentId,
            body.getObject("row").raw().toMap(), response)
            .thenAccept(value -> futureResponse.complete(null)))), () -> {});
    return futureResponse;
  }

  private CompletableFuture<Void> insertTableEntry(
    TableEntry tableEntry, Table table, UUID contentId,
    Map<String, Object> rowContent, HttpServletResponse response
  ) {
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("id", contentId));
    for (var entry : rowContent.entrySet()) {
      cells.add(TableCell.create(entry.getKey(), entry.getValue()));
    }
    coreModule.triggerWorkflows("table", "database-entry-insert-trigger",
      "tableId='" + tableEntry.id() + "'", tableInsertInformation(table, contentId));
    return table.insertContent(TableRow.create(cells))
      .thenAccept(success -> response.setStatus(success ?
        HttpServletResponse.SC_OK : HttpServletResponse.SC_BAD_REQUEST));
  }

  private Map<String, Object> tableInsertInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  @RequestMapping(path = "/table/entry/update/", method = RequestMethod.POST)
  public CompletableFuture<Void> updateTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var futureResponse = new CompletableFuture<Void>();
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableEntry).thenAccept(table ->
        updateTableEntry(table, body.getUUID("row"),
          body.getObject("content").raw().toMap(), response)
          .thenAccept(value -> futureResponse.complete(null))), () -> {});
    return futureResponse;
  }

  private CompletableFuture<Void> updateTableEntry(
    Table table, UUID rowId, Map<String, Object> rowContent,
    HttpServletResponse response
  ) {
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("id", rowId));
    for (var entry : rowContent.entrySet()) {
      cells.add(TableCell.create(entry.getKey(), entry.getValue()));
    }
    return table.updateContent(rowId, TableRow.create(cells))
      .thenAccept(success -> response.setStatus(success ?
        HttpServletResponse.SC_OK : HttpServletResponse.SC_BAD_REQUEST));
  }

  @RequestMapping(path = "/table/entry/remove/", method = RequestMethod.POST)
  public void removeTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    performTableOperation(findUserId(request), body.getString("table"),
      tableEntry -> tableFactory.create(tableEntry).thenAccept(table ->
        removeTableEntry(tableEntry, table, body.getUUID("row"))), () -> {});
  }

  private void removeTableEntry(TableEntry tableEntry, Table table, UUID rowId) {
    table.removeContent(rowId);
    coreModule.triggerWorkflows("table", "database-entry-remove-trigger",
      "tableId='" + tableEntry.id() + "'", tableRemoveInformation(table, rowId));
  }

  private Map<String, Object> tableRemoveInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  @RequestMapping(path = "/table/column/add/", method = RequestMethod.POST)
  public void addTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName");
    if (columnName.equalsIgnoreCase("id")) {
      return;
    }
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableEntry).thenAccept(table ->
        addTableColumn(table, columnName)), () -> {});
  }

  private void addTableColumn(Table table, String columnName) {
    if (table.columns().stream().anyMatch(column -> column.name().equals(columnName))) {
      return;
    }
    table.addColumn(DatabaseColumn.create(columnName, DatabaseDataType.TEXT));
  }

  @RequestMapping(path = "/table/column/remove/", method = RequestMethod.POST)
  public void removeTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName");
    if (columnName.equalsIgnoreCase("id")) {
      return;
    }
    performTableOperation(findUserId(request), tableId, tableEntry ->
      tableFactory.create(tableEntry).thenAccept(table ->
        removeTableColumn(table, columnName)), () -> {});
  }

  private void removeTableColumn(Table table, String columnName) {
    if (table.columns().stream().noneMatch(column -> column.name().equals(columnName))) {
      return;
    }
    table.dropColumn(columnName);
  }

  @RequestMapping(path = "/table/rename/", method = RequestMethod.POST)
  public void renameTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var tableName = body.getString("name");
    if (tableName.replace(" ", "").isEmpty()) {
      return;
    }
    performTableOperation(findUserId(request), tableId, table ->
      tableDatabaseTable().changeTableName(table, tableName), () -> {});
  }

  @RequestMapping(path = "/table/delete/", method = RequestMethod.POST)
  public void deleteTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    performTableOperation(findUserId(request), body.getString("table"),
      this::deleteTable, () -> {});
  }

  public void deleteTable(TableEntry tableEntry) {
    var tableId = tableEntry.id();
    tableDatabaseTable().deleteTable(tableId);
    tableFactory.create(tableEntry).thenAccept(DatabaseTable::dropIfExists);
  }
}
