package net.taskwolf.table.access;

import net.taskwolf.core.access.TaskwolfRequestBody;
import net.taskwolf.core.database.aggregation.DatabaseAggregation;
import net.taskwolf.core.database.paging.DatabaseDirection;
import net.taskwolf.core.database.paging.DatabaseOrder;
import net.taskwolf.core.database.paging.DatabasePage;
import net.taskwolf.core.iterator.AsyncIterator;
import net.taskwolf.core.organization.team.TeamTargetDatabaseTable;
import net.taskwolf.core.user.User;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
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
        .thenAccept(table -> detailedTableInformation(entry, table)
          .thenAccept(futureResponse::complete)),
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
    TableEntry entry, Table table
  ) {
    return userDatabaseTable().findUserIfExists(entry.creator())
      .thenApply(creator -> assemblyDetailedTableInformation(entry, creator, table));
  }

  private Map<String, Object> assemblyDetailedTableInformation(
    TableEntry entry, User creator, Table table
  ) {
    var information = superficialTableInformation(entry, creator);
    information.put("columns", assemblyTableColumnsInformation(table.tableColumns()));
    return information;
  }

  private List<Map<String, Object>> assemblyTableColumnsInformation(
    List<TableColumn> columns
  ) {
    var result = Lists.<Map<String, Object>>newArrayList();
    for (var column : columns) {
      var columnInformation = Maps.<String, Object>newHashMap();
      columnInformation.put("id", column.id());
      columnInformation.put("type", column.type());
      columnInformation.put("name", column.name());
      result.add(columnInformation);
    }
    return result;
  }

  @RequestMapping(path = "/table/content/page/first/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> firstTableContentPage(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var pageSize = body.getInt("pageSize");
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    findUser(request).thenAccept(user -> performTableOperation(user,
      body.getString("table"), entry -> tableFactory.create(entry)
        .thenCompose(table -> table.firstContentPage(pageSize))
        .thenApply(this::collectContentInformation)
        .thenAccept(futureResponse::complete),
      () -> futureResponse.complete(Maps.newHashMap())));
    return futureResponse;
  }

  @RequestMapping(path = "/table/content/page/next/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> nextTableContentPage(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var pageState = body.getString("pageState");
    var pageSize = body.getInt("pageSize");
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    findUser(request).thenAccept(user -> performTableOperation(user,
      body.getString("table"), entry -> tableFactory.create(entry)
        .thenCompose(table -> table.nextContentPage(pageState, pageSize))
        .thenApply(this::collectContentInformation)
        .thenAccept(futureResponse::complete),
      () -> futureResponse.complete(Maps.newHashMap())));
    return futureResponse;
  }

  private Map<String, Object> collectContentInformation(
    DatabasePage<TableRow> page
  ) {
    if (page.content().isEmpty()) {
      return Map.of("content", Lists.newArrayList(), "page", page.pageState());
    }
    return Map.of("content", assemblyTableRowsInformation(page.content()),
      "page", page.pageState());
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

  @RequestMapping(path = "/table/download/", method = RequestMethod.POST)
  public CompletableFuture<ResponseEntity<InputStreamResource>> downloadTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var futureResponse = new CompletableFuture<ResponseEntity<InputStreamResource>>();
    findUser(request).thenAccept(user -> performTableOperation(user,
      body.getString("table"), entry -> tableFactory.create(entry)
        .thenCompose(table -> table.download()
          .thenApply(file -> streamTableContent(entry, file)))
        .thenAccept(futureResponse::complete),
      () -> futureResponse.complete(ResponseEntity.notFound().build())));
    return futureResponse;
  }

  private ResponseEntity<InputStreamResource> streamTableContent(
    TableEntry tableEntry, File file
  ) {
    try {
      var fileInputStream = new FileInputStream(file);
      var resource = new InputStreamResource(fileInputStream);
      var response = ResponseEntity.ok()
        .header(HttpHeaders.CONTENT_DISPOSITION,
          "attachment; filename=\"" + tableEntry.name() + ".csv\"")
        .body(resource);
      file.delete();
      return response;
    } catch (Exception exception) {
      exception.printStackTrace();
      return ResponseEntity.notFound().build();
    }
  }

  @RequestMapping(path = "/table/aggregate/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> aggregateTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var aggregation = DatabaseAggregation.valueOf(body.getString("aggregation"));
    var columnId = body.getString("column");
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    findUser(request).thenAccept(user -> performTableOperation(user,
      body.getString("table"), entry -> tableFactory.create(entry)
        .thenAccept(table -> aggregateTable(entry, table, aggregation, columnId)
          .thenAccept(futureResponse::complete)),
      () -> futureResponse.complete(Maps.newHashMap())));
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> aggregateTable(
    TableEntry entry, Table table, DatabaseAggregation aggregation, String columnId
  ) {
    if (!entry.columns().contains(columnId)) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1000));
    }
    var column = table.tableColumns().stream()
      .filter(tableColumn -> tableColumn.id().equals(columnId)).findFirst().get();
    if (column.type() != TableColumnType.NUMBER) {
      return CompletableFuture.completedFuture(Map.of("success", false,
        "errorCode", 1001));
    }
    return table.aggregateContent(aggregation, columnId)
      .thenApply(result -> Map.of("success", true, "result", result));
  }

  private String timeMillisecondsToDate(long milliseconds) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(milliseconds);
    return simpleDateFormat.format(calendar.getTime());
  }
}

