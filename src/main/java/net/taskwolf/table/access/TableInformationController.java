package net.taskwolf.table.access;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jakarta.servlet.http.HttpServletRequest;
import net.taskwolf.core.access.TaskwolfRestController;
import net.taskwolf.core.database.DatabaseColumn;
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
      target -> tableDatabaseTable.findTablesOfOwner(target).thenAccept(tables ->
        futureResponse.complete(Map.of("tables", tables.stream()
          .map(this::superficialTableInformation).toList()))));
    return futureResponse;
  }

  @RequestMapping(path = "/table/find/", method = RequestMethod.POST)
  public CompletableFuture<Map<String, Object>> findTable(
    HttpServletRequest request, @RequestBody Map<String, Object> input
  ) {
    var futureResponse = new CompletableFuture<Map<String, Object>>();
    var tableId = UUID.fromString((String) input.get("table"));
    var page = Integer.parseInt((String) input.get("page"));
    var userId = findUserId(request);
    tableDatabaseTable.tableExists(tableId).thenAccept(exists ->
      findTable(userId, tableId, page, exists).thenAccept(futureResponse::complete));
    return futureResponse;
  }

  private CompletableFuture<Map<String, Object>> findTable(
    UUID userId, UUID tableId, int page, boolean exists
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

  private Map<String, Object> superficialTableInformation(TableEntry table) {
    var information = Maps.<String, Object>newHashMap();
    information.put("id", table.id());
    information.put("title", table.name());
    return information;
  }

  private static final int TABLE_PAGE_SIZE = 10;

  private CompletableFuture<Map<String, Object>> detailedTableInformation(
    TableEntry entry, Table table, int page
  ) {
    return table.findContent(TABLE_PAGE_SIZE, page).thenApply(rows ->
      assemblyDetailedTableInformation(entry, table, rows));
  }

  private Map<String, Object> assemblyDetailedTableInformation(
    TableEntry entry, Table table, List<TableRow> rows
  ) {
    var information = superficialTableInformation(entry);
    information.put("columns", assemblyTableColumnsInformation(table.columns()));
    information.put("rows", assemblyTableRowsInformation(rows));
    return information;
  }

  private List<Map<String, Object>> assemblyTableColumnsInformation(
    List<DatabaseColumn> columns
  ) {
    var result = Lists.<Map<String, Object>>newArrayList();
    for (var column : columns) {
      var columnInformation = Maps.<String, Object>newHashMap();
      columnInformation.put("name", column.name());
      columnInformation.put("type", column.type());
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

