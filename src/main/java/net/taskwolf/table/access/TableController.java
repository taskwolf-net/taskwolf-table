package net.taskwolf.table.access;

import net.taskwolf.core.access.TaskwolfRestController;
import net.taskwolf.core.organization.team.TeamTargetDatabaseTable;
import net.taskwolf.core.user.User;
import net.taskwolf.core.user.UserDatabaseTable;
import net.taskwolf.core.user.UserTargetDatabaseTable;
import net.taskwolf.table.structure.TableDatabaseTable;
import net.taskwolf.table.structure.TableEntry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.security.Key;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Accessors(fluent = true)
@Getter(AccessLevel.PROTECTED)
public class TableController extends TaskwolfRestController {
  private final TableDatabaseTable tableDatabaseTable;
  private final UserTargetDatabaseTable userTargetDatabaseTable;
  private final TeamTargetDatabaseTable teamTargetDatabaseTable;

  protected TableController(
    Key secretKey, UserDatabaseTable userDatabaseTable,
    TableDatabaseTable tableDatabaseTable,
    UserTargetDatabaseTable userTargetDatabaseTable,
    TeamTargetDatabaseTable teamTargetDatabaseTable
  ) {
    super(secretKey, userDatabaseTable);
    this.tableDatabaseTable = tableDatabaseTable;
    this.userTargetDatabaseTable = userTargetDatabaseTable;
    this.teamTargetDatabaseTable = teamTargetDatabaseTable;
  }

  protected void performTableOperation(
    UUID userId, String tableId, Consumer<TableEntry> operation,
    Runnable failResponse
  ) {
    userDatabaseTable().findUser(userId).thenAccept(user ->
      performTableOperation(user, tableId, operation, failResponse));
  }

  protected void performTableOperation(
    User user, String tableId, Consumer<TableEntry> operation,
    Runnable failResponse
  ) {
    tableDatabaseTable.tableExists(tableId).thenAccept(exists ->
      performTableOperation(user, tableId, exists, operation,
        failResponse));
  }

  private void performTableOperation(
    User user, String tableId, boolean tableExists,
    Consumer<TableEntry> operation, Runnable failResponse
  ) {
    if (!tableExists) {
      failResponse.run();
      return;
    }
    tableDatabaseTable.findTable(tableId).thenAccept(table ->
      checkTableAuthorization(user, table).thenAccept(authorized ->
        performTableOperation(table, authorized, operation, failResponse)));
  }

  private void performTableOperation(
    TableEntry table, boolean authorized, Consumer<TableEntry> operation,
    Runnable failResponse
  ) {
    if (!authorized) {
      failResponse.run();
      return;
    }
    operation.accept(table);
  }

  protected CompletableFuture<Boolean> checkTableAuthorization(
    User user, TableEntry table
  ) {
    return checkTableAuthorization(user, table.owner());
  }

  protected CompletableFuture<Boolean> checkTableAuthorization(
    User user, UUID tableOwnerId
  ) {
    if (tableOwnerId.equals(user.id()) ||
      user.organizations().contains(tableOwnerId)
    ) {
      return CompletableFuture.completedFuture(true);
    }
    return teamTargetDatabaseTable.findTargetSecured(user.id())
      .thenApply(teamTarget -> teamTarget.map(uuid ->
        uuid.equals(tableOwnerId)).orElse(false));
  }

  protected CompletableFuture<UUID> findTableTarget(UUID userId) {
    return userTargetDatabaseTable.findTargetSecured(userId)
      .thenCompose(target -> findTableTarget(userId, target));
  }

  private CompletableFuture<UUID> findTableTarget(
    UUID userId, UUID target
  ) {
    return userId.equals(target) ? CompletableFuture.completedFuture(target) :
      teamTargetDatabaseTable.findTargetSecured(userId)
        .thenApply(team -> team.orElse(target));
  }
}
