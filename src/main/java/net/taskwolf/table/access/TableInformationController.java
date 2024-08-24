package net.taskwolf.table.access;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.taskwolf.core.access.TaskwolfRequestBody;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.database.DatabaseDirection;
import net.taskwolf.core.database.DatabaseOrder;
import net.taskwolf.core.database.DatabasePage;
import net.taskwolf.core.iterator.AsyncIterator;
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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
public final class TableInformationController extends TableController {
  private final TableFactory tableFactory;
  private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd.MM.yyyy");

  private TableInformationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable,
    TableFactory tableFactory
  ) {
    super(secretKey, userDatabaseTable, tableDatabaseTable,
      userTargetDatabaseTable, teamTargetDatabaseTable);
    this.tableFactory = tableFactory;
  }

  @RequestMapping(path = "/tables/page/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> findTablePage(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var targetPage = body.getInt("targetPage");
    var sortingColumn = body.getString("sorting");
    var sortingOrder = DatabaseOrder.valueOf(body.getString("order"));
    var search = body.getString("search");
    var creatorId = body.has("creator") ? body.getUUID("creator") : null;
    var startTime = body.has("startTime") ? body.getLong("startTime") : -1;
    var endTime = body.has("endTime") ? body.getLong("endTime") : -1;
    var minimumSize = body.has("minimumSize") ? body.getLong("minimumSize") : -1;
    var maximumSize = body.has("maximumSize") ? body.getLong("maximumSize") : -1;
    return findTableTarget(findUserId(request)).thenCompose(target ->
      tableDatabaseTable().findTablesOfOwner(target, targetPage,
          sortingColumn, sortingOrder, search, creatorId, startTime, endTime,
          minimumSize, maximumSize)
        .thenCompose(this::collectTableInformation));
  }

  @RequestMapping(path = "/tables/page/shift/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> shiftTablePage(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var pageState = body.getString("pageState");
    var startingPoint = DatabaseDirection.valueOf(body.getString("startingPoint"));
    var direction = DatabaseDirection.valueOf(body.getString("direction"));
    var sortingColumn = body.getString("sorting");
    var sortingOrder = DatabaseOrder.valueOf(body.getString("order"));
    var creatorId = body.has("creator") ? body.getUUID("creator") : null;
    var startTime = body.has("startTime") ? body.getLong("startTime") : -1;
    var endTime = body.has("endTime") ? body.getLong("endTime") : -1;
    var minimumSize = body.has("minimumSize") ? body.getLong("minimumSize") : -1;
    var maximumSize = body.has("maximumSize") ? body.getLong("maximumSize") : -1;
    return findTableTarget(findUserId(request)).thenCompose(target ->
      tableDatabaseTable().findTablesOfOwner(target, pageState,
          startingPoint, direction, sortingColumn, sortingOrder, creatorId,
          startTime, endTime, minimumSize, maximumSize)
        .thenCompose(this::collectTableInformation));
  }

  private CompletableFuture<Map<String, Object>> collectTableInformation(
    DatabasePage<TableEntry> page
  ) {
    if (page.content().isEmpty()) {
      return CompletableFuture.completedFuture(Map.of("tables",
        Lists.newArrayList(), "page", page.pageState(), "pageNumber", 0));
    }
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    AsyncIterator.execute(page.content(), this::gatherTableInformation)
      .thenApply(information -> reconstructTableOrder(page, information))
      .thenAccept(information -> futureResponse.complete(Map.of("tables",
        information, "page", page.pageState(), "pageNumber", page.pageNumber())));
    return futureResponse;
  }

  private List<Map<String, Object>> reconstructTableOrder(
    DatabasePage<TableEntry> page, List<Map<String, Object>> information
  ) {
    var result = Lists.<Map<String, Object>>newArrayList();
    for (var table : page.content()) {
      for (var entry : information) {
        if (table.id().toString().equals(entry.get("id").toString())) {
          result.add(entry);
          break;
        }
      }
    }
    return result;
  }

  private CompletableFuture<Map<String, Object>> gatherTableInformation(
    TableEntry table
  ) {
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    userDatabaseTable().findUserIfExists(table.creator())
      .thenAccept(creator -> futureResponse.complete(
        superficialTableInformation(table, creator)));
    return futureResponse;
  }

  @RequestMapping(path = "/table/find/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> findTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    findUser(request).thenAccept(user -> performTableOperation(user,
      body.getString("table"), entry -> tableFactory.create(entry)
        .thenAccept(table -> detailedTableInformation(entry, table,
          body.getInt("page")).thenAccept(futureResponse::complete)),
      () -> futureResponse.complete(Maps.newHashMap())));
    return futureResponse;
  }

  private Map<String, Object> superficialTableInformation(
    TableEntry entry, User creator
  ) {
    var information = Maps.<String, Object>newHashMap();
    information.put("id", entry.id());
    information.put("name", entry.name());
    information.put("creator", creator.name());
    information.put("created", timeMillisecondsToDate(entry.created()));
    information.put("size", formatByteSize(entry.size()));
    return information;
  }

  private String formatByteSize(long size) {
    if (size < 1024) {
      return size + " B";
    }
    int z = (63 - Long.numberOfLeadingZeros(size)) / 10;
    return String.format("%.1f %sB", (double) size / (1L << (z * 10)),
      " KMGTPE".charAt(z));
  }

  private CompletableFuture<Map<String, Object>> detailedTableInformation(
    TableEntry entry, Table table, int page
  ) {
    return table.findContent(TABLE_PAGE_SIZE, page)
      .thenCompose(rows -> userDatabaseTable().findUserIfExists(entry.creator())
        .thenApply(creator -> assemblyDetailedTableInformation(entry, creator,
          table, rows)));
  }

  private Map<String, Object> assemblyDetailedTableInformation(
    TableEntry entry, User creator, Table table, List<TableRow> rows
  ) {
    var information = superficialTableInformation(entry, creator);
    information.put("columns", assemblyTableColumnsInformation(table.columns()));
    information.put("rows", assemblyTableRowsInformation(rows));
    return information;
  }

  private List<Map<String, Object>> assemblyTableColumnsInformation(
    List<DatabaseColumn> columns
  ) {
    var result = Lists.<Map<String, Object>>newArrayList();
    for (var column : columns) {
      if (column.name().equalsIgnoreCase("id")) {
        continue;
      }
      var columnInformation = Maps.<String, Object>newHashMap();
      columnInformation.put("name", column.name());
      columnInformation.put("type", column.dataType());
      result.add(columnInformation);
    }
    return result;
  }

  private List<Map<String, Object>> assemblyTableRowsInformation(List<TableRow> rows) {
    var result = Lists.<Map<String, Object>>newArrayList();
    for (var row : rows) {
      var columnInformation = Lists.<Map<String, Object>>newArrayList();
      for (var cell : row.cells()) {
        var cellInformation = Maps.<String, Object>newHashMap();
        cellInformation.put("column", cell.column());
        cellInformation.put("value", cell.value());
        columnInformation.add(cellInformation);
      }
      result.add(Map.of("cells", columnInformation));
    }
    return result;
  }

  private String timeMillisecondsToDate(long milliseconds) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(milliseconds);
    return simpleDateFormat.format(calendar.getTime());
  }
}

