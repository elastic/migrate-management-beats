# migrate-management-beats [WIP]

This tool migrates Beats Central Management index in Elasticsearch from 6.6 to 6.7.

## Usage

To migrate the Central Management index, after editing the configuration, run the following command.

```
./migrate-management-beats
```

The migration is done in four steps:

1. Backup existing `.management-beats` index into `.management-beats-backup`. Then it is aliased as `.management-beats`.
2. A new temporary index is created to migrate old documents here.
3. Migrate documents from `.management-beats`
4. Finalize migration by removing the alias, moving documents from the new index to `.management-beats`. Furthermore, delete
temporary indices.

If one of the steps fail, the changes of the problematic step is rolled back. After manually addressing the issues of migrating
the migration can be continued from that step.

```
./migrate-management-beats --step {step}
```

For example if the second step fails, because the index already exists, it can be continued. First, delete the index and let
the script run from the second step to create the appropiate intermediary index.

```
./migrate-management-beats --step 2
```

## Configuration file

The configuration file is named `migrate.yml`. You can configure `url`, `username` and `password` along with `ssl.*` settings.
