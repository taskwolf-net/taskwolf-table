package net.taskwolf.table.access;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import net.taskwolf.core.access.TaskwolfRequestBody;
import net.taskwolf.core.access.TaskwolfRestController;
import net.taskwolf.core.database.DatabaseColumn;
import net.taskwolf.core.iterator.AsyncIterator;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.*;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.security.Key;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@RestController
public final class TableInformationController extends TaskwolfRestController {
  private final TableDatabaseTable tableDatabaseTable;
  private final TableFactory tableFactory;
  private final UserTargetDatabaseTable userTargetDatabaseTable;

  private TableInformationController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable, TableFactory tableFactory,
    UserTargetDatabaseTable userTargetDatabaseTable
  ) {
    super(secretKey, userDatabaseTable);
    this.tableDatabaseTable = tableDatabaseTable;
    this.tableFactory = tableFactory;
    this.userTargetDatabaseTable = userTargetDatabaseTable;
  }

  @RequestMapping(path = "/tables/find/", method = RequestMethod.GET)
  public CompletableFuture<Map<String, Object>> findTables(
    HttpServletRequest request
  ) {
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    userTargetDatabaseTable.findTargetSecured(findUserId(request)).thenAccept(
      target -> tableDatabaseTable.findTablesOfOwner(target).thenAccept(entries ->
        findTables(entries).thenAccept(futureResponse::complete)));
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> findTables(
    List<TableEntry> entries
  ) {
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    AsyncIterator.execute(entries, entry -> tableFactory.create(entry.id())
        .thenCompose(table -> table.count().thenCompose(rows ->
          table.averageRowSize().thenApply(averageRowSize ->
            superficialTableInformation(entry, rows, averageRowSize)))),
      entries.size(), tables -> futureResponse.complete(Map.of("tables", tables)));
    return futureResponse;
  }

  @RequestMapping(path = "/table/find/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> findTable(
    HttpServletRequest request, @RequestBody String payload,
    HttpServletResponse response
  ) {
    var body = TaskwolfRequestBody.of(payload, response);
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    var tableId = body.getString("table");
    var userId = findUserId(request);
    tableDatabaseTable.tableExists(tableId)
      .thenAccept(exists -> findTable(userId, tableId, body.getInt("page"), exists)
        .thenAccept(futureResponse::complete));
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> findTable(
    UUID userId, String tableId, int page, boolean exists
  ) {
    if (!exists) {
      return CompletableFuture.completedFuture(Maps.newHashMap());
    }
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    tableDatabaseTable.findTable(tableId).thenAccept(table ->
      userTargetDatabaseTable.findTargetSecured(userId).thenAccept(target ->
        findTable(table, target, page).thenAccept(futureResponse::complete)));
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> findTable(
    TableEntry entry, UUID target, int page
  ) {
    if (!entry.owner().equals(target)) {
      return CompletableFuture.completedFuture(Maps.newHashMap());
    }
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    tableFactory.create(entry.id()).thenAccept(table ->
      detailedTableInformation(entry, table, page)
        .thenAccept(futureResponse::complete));
    return futureResponse;
  }

  private Map<String, Object> superficialTableInformation(
    TableEntry entry, long rowNumber, long averageRowSize
  ) {
    var information = Maps.<String, Object>newHashMap();
    information.put("id", entry.id());
    information.put("name", entry.name());
    information.put("entries", rowNumber);
    information.put("size", formatByteSize(rowNumber * averageRowSize));
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

  private static final int TABLE_PAGE_SIZE = 5;

  private CompletableFuture<Map<String, Object>> detailedTableInformation(
    TableEntry entry, Table table, int page
  ) {
    return table.count().thenCompose(rowNumber -> table.averageRowSize()
      .thenCompose(averageRowSize -> table.findContent(TABLE_PAGE_SIZE, page)
        .thenApply(rows -> assemblyDetailedTableInformation(entry, table,
          rowNumber, averageRowSize, page, rows))));
  }

  private Map<String, Object> assemblyDetailedTableInformation(
    TableEntry entry, Table table, long totalRowNumber, long averageRowSize,
    int page, List<TableRow> rows
  ) {
    var information = superficialTableInformation(entry, totalRowNumber,
      averageRowSize);
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
}

