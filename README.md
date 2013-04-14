# Bicod

### Binary Compare Databases

Binary search for missing records between two databases.

## Using as CLI tool

```bash
$ bicod SourceURI TargetURI
```

where `URI` can be:

* for MongoDB - `mongo://host:port/database/collection/field`
    example: `mongo://localhost:27107/blog/posts/_id`

* for ElasticSearch - `es://host:port/index/type/field`
    example: `es://localhost:9200/blog/post/id`


Full example:

```bash
$ bicod mongo://localhost:27107/blog/posts/_id es://localhost:9200/blog/post/id
```

## Using as library

```hs
import Database.Bicod

main = do
  missing <- runOps (mongoOps defaultMongo) (esOps defaultElasticSearch)
  -- missing is a Set of missing record fields 
```


## Building

```bash
$ cabal configure
$ cabal build
```
