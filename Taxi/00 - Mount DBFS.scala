// Databricks notebook source
//dbutils.fs.mount(
//  source = "wasbs://data@techobricks.blob.core.windows.net/",
//  mountPoint = "/mnt/data",
//  extraConfigs = Map("fs.azure.account.key.techobricks.blob.core.windows.net" -> "mystoragekey"))

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "clientid",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "key-vault-secrets", key = "adfspnkey"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

dbutils.fs.mount(
  source = "abfss://data@nawebricks.dfs.core.windows.net/",
  mountPoint = "/mnt/adlsgen2",
  extraConfigs = configs)



// COMMAND ----------

 dbutils.fs.unmount("/mnt/adlsgen2")