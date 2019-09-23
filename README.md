# azure-databricks-playground

We used Azure Key Vault to store the secret for connecting to ADLS Gen2 https://docs.azuredatabricks.net/user-guide/secrets/secret-scopes.html#create-an-azure-key-vault-backed-secret-scope.  An even better solution today is probably to use AAD Passthrough security with ADLS Gen2: https://docs.azuredatabricks.net/administration-guide/cloud-configurations/azure/credential-passthrough.html.
