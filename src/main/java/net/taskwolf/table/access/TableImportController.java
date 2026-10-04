package net.taskwolf.table.access;

import net.taskwolf.core.bundle.BundleDatabaseTable;
import net.taskwolf.core.database.DatabaseDataType;
import net.taskwolf.core.error.ErrorRepository;
import net.taskwolf.core.iterator.AsyncIterator;
import net.taskwolf.core.organization.team.Team;
import net.taskwolf.core.organization.team.TeamDatabaseTable;
import net.taskwolf.core.organization.team.TeamTargetDatabaseTable;
import net.taskwolf.core.user.User;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.*;
import com.google.common.collect.Lists;
import jakarta.servlet.http.HttpServletRequest;
import org.json.JSONObject;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@RestController
public final class TableImportController extends TableController {
  private final TableFactory tableFactory;
  private final TableColumnDatabaseTable tableColumnDatabaseTable;
  private final BundleDatabaseTable bundleDatabaseTable;
  private final TeamDatabaseTable teamDatabaseTable;
  private final ErrorRepository errorRepository;
  private final PolicyFactory sanitizerPolicy = new HtmlPolicyBuilder().toFactory();

  private TableImportController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable,
    TableFactory tableFactory, TableColumnDatabaseTable tableColumnDatabaseTable,
    BundleDatabaseTable bundleDatabaseTable, TeamDatabaseTable teamDatabaseTable,
    ErrorRepository errorRepository
  ) {
    super(secretKey, userDatabaseTable, tableDatabaseTable,
      userTargetDatabaseTable, teamTargetDatabaseTable);
    this.tableFactory = tableFactory;
    this.tableColumnDatabaseTable = tableColumnDatabaseTable;
    this.bundleDatabaseTable = bundleDatabaseTable;
    this.teamDatabaseTable = teamDatabaseTable;
    this.errorRepository = errorRepository;
  }

  @RequestMapping(path = "/table/import/", method = RequestMethod.POST)
  public SseEmitter importTable(
    HttpServletRequest request, @RequestParam("name") String name,
    @RequestParam("file") MultipartFile file
  ) {
    var response = ((ServletRequestAttributes)
      RequestContextHolder.currentRequestAttributes()).getResponse();
    if (response != null) {
      response.setHeader("Cache-Control", "no-store");
      response.setHeader("Connection", "keep-alive");
      response.setHeader("X-Accel-Buffering", "no");
    }
    var emitter = new SseEmitter(-1L);
    var formattedName = sanitizerPolicy.sanitize(
      name.substring(0, Math.min(64, name.length())));
    if (formattedName.isEmpty() || file.isEmpty()) {
      emitter.complete();
      return emitter;
    }
    importTable(request, formattedName, file, emitter);
    return emitter;
  }

  private void importTable(
    HttpServletRequest request, String name, MultipartFile file,
    SseEmitter emitter
  ) {
    findUser(request)
      .thenAccept(user -> userTargetDatabaseTable().findTargetSecured(user.id())
        .thenAccept(target -> findTableOwner(user, target)
          .thenAccept(owner -> tableDatabaseTable().generateAvailableTableId()
            .thenAccept(tableId -> checkDatabaseNumberLimit(user, target)
              .thenAccept(limitReached -> importTable(tableId, owner, user.id(),
                name, limitReached, file, emitter))))));
  }

  private void importTable(
    String tableId, UUID owner, UUID creator, String name, boolean limitReached,
    MultipartFile file, SseEmitter emitter
  ) {
    if (limitReached) {
      sendEmitterMessage(emitter, Map.of("type", "LIMIT"));
      emitter.complete();
      return;
    }
    try {
      var tempFile = createImportTempFile(tableId);
      file.transferTo(tempFile.toFile());
      importTable(tableId, owner, creator, name, tempFile, emitter);
    } catch (Exception exception) {
      try {
        Files.delete(createImportTempFile(tableId));
      } catch (Exception ignored) {
      }
      emitter.complete();
    }
  }

  private void importTable(
    String tableId, UUID owner, UUID creator, String name,
    Path tempFile, SseEmitter emitter
  ) throws Exception {
    var lines = Files.lines(tempFile).count() - 1;
    var reader = new BufferedReader(new FileReader(tempFile.toFile()));
    var rawHeader = reader.readLine().replaceAll("\"", "").split(",");
    var parsedHeader = parseImportHeader(rawHeader);
    generateColumnIds(parsedHeader.size())
      .thenAccept(columnIds -> importTable(tableId, columnIds, owner,
        creator, name, tempFile, reader, lines, rawHeader, parsedHeader,
        emitter));
  }

  private CompletableFuture<List<String>> generateColumnIds(int number) {
    var futureResponse = new CompletableFuture<List<String>>();
    var ids = Collections.synchronizedList(Lists.<String>newArrayList());
    if (number == 0) {
      futureResponse.complete(ids);
      return futureResponse;
    }
    for (int i = 0; i < number; i++) {
      try {
        tableColumnDatabaseTable.generateAvailableColumnId().thenAccept(ids::add)
          .thenApply(value -> ids.size() == number &&
            futureResponse.complete(ids));
      } catch (Exception ignored) {
      }
    }
    return futureResponse;
  }

  private void importTable(
    String tableId, List<String> columnIds, UUID owner, UUID creator, String name,
    Path tempFile, BufferedReader reader, long lines, String[] rawHeader,
    List<String> parsedHeader, SseEmitter emitter
  ) {
    var entry = TableEntry.create(owner, tableId, creator, name, columnIds,
      System.currentTimeMillis(), 0);
    var columns = Lists.<TableColumn>newArrayList();
    for (var i = 0; i < columnIds.size(); i++) {
      columns.add(parseTableColumn(columnIds.get(i), tableId, parsedHeader.get(i)));
    }
    createImportTable(entry, columns, emitter)
      .thenAcceptAsync(table -> insertLines(entry, table, tempFile, reader, lines,
        Lists.newArrayList(rawHeader), columns, emitter));
  }

  private static final Pattern COLUMN_TYPE_PATTERN =
    Pattern.compile("^(.*)\\(([^)]+)\\)$");

  private TableColumn parseTableColumn(
    String columnId, String tableId, String header
  ) {
    try {
      Matcher matcher = COLUMN_TYPE_PATTERN.matcher(header);
      if (matcher.find()) {
        return TableColumn.create(columnId, tableId,
          TableColumnType.valueOf(matcher.group(2)), matcher.group(1).trim());
      }
      return TableColumn.create(columnId, tableId, TableColumnType.TEXT, header);
    } catch (Exception exception) {
      return TableColumn.create(columnId, tableId, TableColumnType.TEXT, header);
    }
  }

  private Path createImportTempFile(String tableId) throws Exception {
    var tempDir = Files.createTempDirectory("database-imports");
    return tempDir.resolve(tableId + ".csv");
  }

  private void insertLines(
    TableEntry entry, Table table, Path file, BufferedReader reader, long lines,
    List<String> rawHeader, List<TableColumn> columns, SseEmitter emitter
  ) {
    sendEmitterMessage(emitter, Map.of("type", "ROWS", "number", lines));
    try {
      var time = System.currentTimeMillis();
      var lineIndex = 0L;
      String line;
      while ((line = reader.readLine()) != null) {
        lineIndex++;
        sendEmitterMessage(emitter, Map.of("type", "INSERTION",
          "progress", lineIndex));
        if (!processLine(entry, table, rawHeader, columns, line, time, lineIndex).join()) {
          break;
        }
      }
    } catch (Exception ignored) {
    }
    finishImport(entry, file, reader, emitter);
  }

  private void finishImport(
    TableEntry entry, Path file, BufferedReader reader, SseEmitter emitter
  ) {
    try {
      reader.close();
    } catch (Exception ignored) {
    }
    sendEmitterMessage(emitter, Map.of("type", "FINISH", "table", entry.id()));
    try {
      Files.delete(file);
    } catch (Exception ignored) {
    }
    emitter.complete();
  }

  private CompletableFuture<Boolean> processLine(
    TableEntry tableEntry, Table table, List<String> rawHeader,
    List<TableColumn> columns, String line, long time, long lineIndex
  ) {
    var data = Pattern.compile("\"(.*?)\"").matcher(line).results()
      .map(match -> match.group(1)).toArray(String[]::new);
    if (data.length != rawHeader.size()) {
      return CompletableFuture.completedFuture(true);
    }
    var entryTime = time - lineIndex;
    return table.generateAvailableContentId()
      .thenCompose(contentId -> insertLine(tableEntry, table, rawHeader,
        columns, data, contentId, entryTime));
  }

  private CompletableFuture<Boolean> insertLine(
    TableEntry tableEntry, Table table, List<String> rawHeader,
    List<TableColumn> columns, String[] data, UUID contentId, long time
  ) {
    var cells = Lists.<TableCell>newArrayList();
    cells.add(TableCell.create("owner", tableEntry.owner()));
    cells.add(TableCell.create("timestamp", time));
    cells.add(TableCell.create("id", contentId));
    var columnIndex = 0;
    for (var i = 0; i < data.length; i++) {
      var rawColumn = rawHeader.get(i);
      if (rawColumn.equals("taskwolf_timestamp") || rawColumn.equals("taskwolf_id")) {
        continue;
      }
      cells.add(createTableCell(columns.get(columnIndex), data[i]));
      columnIndex++;
    }
    return table.insertContent(TableRow.create(errorRepository, cells));
  }

  private TableCell createTableCell(TableColumn column, String data) {
    try {
      data = data.replaceAll("\"", "");
      if (column.type().dataType() == DatabaseDataType.BOOLEAN) {
        return TableCell.create(column.id(), Boolean.parseBoolean(data));
      }
      if (column.type().dataType() == DatabaseDataType.DECIMAL) {
        return TableCell.create(column.id(), new BigDecimal(data));
      }
      return TableCell.create(column.id(), data);
    } catch (Exception exception) {
      if (column.type().dataType() == DatabaseDataType.BOOLEAN) {
        return TableCell.create(column.id(), false);
      } else if (column.type().dataType() == DatabaseDataType.DECIMAL) {
        return TableCell.create(column.id(), null);
      }
      return TableCell.create(column.id(), "");
    }
  }

  private CompletableFuture<Table> createImportTable(
    TableEntry entry, List<TableColumn> tableColumns, SseEmitter emitter
  ) {
    var table = tableFactory.create(entry, tableColumns);
    var processes = Lists.<CompletableFuture<Void>>newArrayList();
    processes.add(tableDatabaseTable().insertTable(entry));
    processes.add(AsyncIterator.execute(tableColumns,
      tableColumnDatabaseTable::insertColumn).thenApply(value -> null));
    processes.add(table.createAsyncIfNotExists()
      .thenApply(value -> table.columns().stream()
        .filter(column -> !column.name().equals("owner"))
        .map(column -> table.createIndexAsyncIfNotExists(column.name())).toList())
      .thenCompose(indexes -> AsyncIterator.execute(indexes, index -> index)
        .thenApply(response -> null)));
    return AsyncIterator.execute(processes, process -> process)
      .thenAccept(value -> sendEmitterMessage(emitter, Map.of("type", "TABLE")))
      .thenApply(value -> table);
  }

  private static final int MAX_TABLE_COLUMNS = 20;

  private List<String> parseImportHeader(String[] header) {
    var result = Lists.<String>newArrayList();
    for (var column : header) {
      if (column.equals("taskwolf_timestamp") || column.equals("taskwolf_id")) {
        continue;
      }
      result.add(column);
      if (result.size() >= MAX_TABLE_COLUMNS) {
        break;
      }
    }
    return result;
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

  private void sendEmitterMessage(SseEmitter emitter, Map<String, Object> content) {
    try {
      emitter.send(new JSONObject(content).toString());
    } catch (Exception ignored) {
    }
  }
}
