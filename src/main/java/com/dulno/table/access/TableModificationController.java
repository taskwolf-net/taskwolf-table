package com.dulno.table.access;

import com.dulno.core.error.ErrorRepository;
import com.dulno.table.structure.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import com.dulno.core.CoreModule;
import com.dulno.core.access.DulnoRequestBody;
import com.dulno.core.bundle.BundleDatabaseTable;
import com.dulno.core.database.DatabaseColumn;
import com.dulno.core.database.DatabaseDataType;
import com.dulno.core.database.DatabaseTable;
import com.dulno.core.database.condition.DatabaseCondition;
import com.dulno.core.iterator.AsyncIterator;
import com.dulno.core.organization.team.Team;
import com.dulno.core.organization.team.TeamDatabaseTable;
import com.dulno.core.organization.team.TeamTargetDatabaseTable;
import com.dulno.core.user.User;
import com.dulno.core.user.UserDatabaseTable;
import com.dulno.core.user.UserTargetDatabaseTable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@RestController
public final class TableModificationController extends TableController {
  private final TableFactory tableFactory;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final CoreModule coreModule;
  private final ErrorRepository errorRepository;

  private TableModificationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable,
    TableFactory tableFactory, BundleDatabaseTable bundleDatabaseTable,
    TeamDatabaseTable teamDatabaseTable, CoreModule coreModule,
    ErrorRepository errorRepository
  ) {
    super(secretKey, userDatabaseTable, tableDatabaseTable,
      userTargetDatabaseTable, teamTargetDatabaseTable);
    this.tableFactory = tableFactory;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.coreModule = coreModule;
    this.errorRepository = errorRepository;
  }

  @RequestMapping(path = "/table/create/", method = RequestMethod.POST)
  public CompletableFuture<Void> createTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var name = body.getString("name", 64);
    if (name.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return findUser(request).thenCompose(user ->
      userTargetDatabaseTable().findTargetSecured(user.id()).thenCompose(target ->
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
    var defaultColumns = Lists.<DatabaseColumn>newArrayList();
    defaultColumns.add(DatabaseColumn.create("owner", DatabaseDataType.UUID,
      DatabaseColumn.Type.PARTITION_KEY));
    defaultColumns.add(DatabaseColumn.create("id", DatabaseDataType.UUID,
      DatabaseColumn.Type.CLUSTERING_KEY));
    defaultColumns.add(DatabaseColumn.create("data", DatabaseDataType.TEXT));
    var table = tableFactory.create(entry, defaultColumns);
    table.createAsyncIfNotExists();
  }

  @RequestMapping(path = "/table/entry/insert/", method = RequestMethod.POST)
  public CompletableFuture<Void> insertTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
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
    cells.add(TableCell.create("owner", tableEntry.owner()));
    cells.add(TableCell.create("id", contentId));
    for (var entry : rowContent.entrySet()) {
      cells.add(TableCell.create(entry.getKey(), entry.getValue()));
    }
    coreModule.triggerWorkflows("table", "database-entry-insert-trigger",
      DatabaseCondition.of("tableId", tableEntry.id()),
      tableInsertInformation(table, contentId));
    return table.insertContent(TableRow.create(errorRepository, cells))
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
    var body = DulnoRequestBody.of(payload, response);
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
    return table.updateContent(rowId, TableRow.create(errorRepository, cells))
      .thenAccept(success -> response.setStatus(success ?
        HttpServletResponse.SC_OK : HttpServletResponse.SC_BAD_REQUEST));
  }

  @RequestMapping(path = "/table/entry/remove/", method = RequestMethod.POST)
  public void removeTableEntry(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    performTableOperation(findUserId(request), body.getString("table"),
      tableEntry -> tableFactory.create(tableEntry).thenAccept(table ->
        removeTableEntry(tableEntry, table, body.getUUID("row"))), () -> {});
  }

  private void removeTableEntry(TableEntry tableEntry, Table table, UUID rowId) {
    table.removeContent(rowId);
    coreModule.triggerWorkflows("table", "database-entry-remove-trigger",
      DatabaseCondition.of("tableId", tableEntry.id()),
      tableRemoveInformation(table, rowId));
  }

  private Map<String, Object> tableRemoveInformation(Table table, UUID entryId) {
    var information = Maps.<String, Object>newHashMap();
    information.put("tableName", table.name());
    information.put("entryId", entryId);
    return information;
  }

  private static final Pattern COLUMN_PATTERN =
    Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

  @RequestMapping(path = "/table/column/add/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> addTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName", 64);
    if (columnName.equalsIgnoreCase("id") || columnName.equalsIgnoreCase("owner")) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1000));
    }
    if (!COLUMN_PATTERN.matcher(columnName).matches()) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1001));
    }
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    performTableOperation(findUserId(request), tableId,
      tableEntry -> tableFactory.create(tableEntry).thenAccept(table ->
        futureResponse.complete(addTableColumn(table, columnName))),
      () -> futureResponse.complete(Map.of("success", false)));
    return futureResponse;
  }

  private Map<String, Object> addTableColumn(Table table, String columnName) {
    if (table.columns().stream().anyMatch(column -> column.name().equals(columnName))) {
      return Map.of("success", false, "errorCode", 1002);
    }
    table.addColumn(DatabaseColumn.create(columnName, DatabaseDataType.TEXT));
    return Map.of("success", true);
  }

  @RequestMapping(path = "/table/column/remove/", method = RequestMethod.POST)
  public void removeTableColumn(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var columnName = body.getString("columnName");
    if (columnName.equalsIgnoreCase("id") || columnName.equalsIgnoreCase("owner")) {
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
    var body = DulnoRequestBody.of(payload, response);
    var tableId = body.getString("table");
    var tableName = body.getString("name", 64);
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
    var body = DulnoRequestBody.of(payload, response);
    performTableOperation(findUserId(request), body.getString("table"),
      this::deleteTable, () -> {});
  }

  public void deleteTable(TableEntry tableEntry) {
    var tableId = tableEntry.id();
    tableDatabaseTable().deleteTable(tableId);
    tableFactory.create(tableEntry).thenAccept(DatabaseTable::dropIfExists);
  }
}
