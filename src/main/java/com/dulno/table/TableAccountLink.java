package com.dulno.table;

import com.dulno.core.account.AccountLink;
import com.dulno.core.account.AccountLinkEntry;
import com.dulno.table.structure.TableDatabaseTable;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "create")
public final class TableAccountLink implements AccountLink {
  private final TableDatabaseTable tableDatabaseTable;

  @Override
  public CompletableFuture<Boolean> accountExists(UUID id) {
    return tableDatabaseTable.tableExistsByOwner(id);
  }

  @Override
  public CompletableFuture<List<AccountLinkEntry>> findAccounts(UUID userId) {
    return CompletableFuture.completedFuture(Lists.newArrayList());
  }

  @Override
  public void removeAccount(UUID userId, String identifier) {

  }

  @Override
  public String registrationUrl(UUID id, String apiKey) {
    return "/database/create/";
  }

  @Override
  public String description() {
    return "table.link.description";
  }
}