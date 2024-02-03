package net.taskwolf.table;

import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import net.taskwolf.core.account.AccountLink;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor(staticName = "create")
public final class TableAccountLink implements AccountLink {
  @Override
  public CompletableFuture<Boolean> accountExists(UUID userId) {
    return CompletableFuture.completedFuture(true);
  }

  @Override
  public CompletableFuture<List<String>> findAccounts(UUID userId) {
    return CompletableFuture.completedFuture(Lists.newArrayList());
  }

  @Override
  public void removeAccount(UUID userId, String identifier) {

  }

  @Override
  public String registrationUrl(UUID id, String apiKey) {
    return "";
  }

  @Override
  public String description() {
    return "Great news! You don't need to link an account to use this module. Just click on \"skip\" to go to the next page.";
  }
}