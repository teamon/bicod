{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}

import Database.Bicod
import Data.Aeson

--bin = ElasticSearch ("requestb.in", 80) "nvn0x2nv" "_id"

main = runOps (mongoOps defaultMongo) (esOps defaultElasticSearch)

--parseURI :: Text -> V DBConfig
--parseURI str = case splitOn "://" str of
--  ["mongo", rest] -> parseMongoURI rest
--  ["es", rest]    -> parseElasticSearchURI rest
--  [driver, _]     -> Left $ "Unknown driver " `append` driver
--  _               -> Left $ "Invalid database URI: " `append` str


---- Example: mongo://host:port/dbname/collection/field
--parseMongoURI :: Text -> V DBConfig
--parseMongoURI uri = case parseURIParts uri of
--  [host, db, coll, field] -> (\h -> Mongo h db coll field) <$> parseHost 27017 host
--  _                       -> Left $ "Invalid MongoDB URI " `append` uri


---- Example: es://host:port/index/field
--parseElasticSearchURI :: Text -> V DBConfig
--parseElasticSearchURI uri = case parseURIParts uri of
--  [host, index, field]    -> (\h -> ElasticSearch h index field) <$> parseHost 9200 host
--  _                       -> Left $ "Invalid ElasticSearch URI " `append` uri


--parseURIParts :: Text -> [Text]
--parseURIParts = splitOn "/"


--parseHost :: Int -> Text -> V (Text, Int)
--parseHost defaultPort str = case splitOn ":" str of
--  [host, port]  -> left pack $ (,) host . fst <$> decimal port
--  [host]        -> Right (host, defaultPort)
--  _             -> Left $ "Invalid host: " `append` str

--main = do
--  print $ parseURI "es://localhost:1234/posts/field"
--  print $ parseURI "mongo://localhost:1234/dbname/collection/field"



--main = esSearchQuery defaultElasticSearch
--main = do
--  print $ decodeEsQueryResult "id" "{\"_shards\":{\"failed\":0,\"successful\":5,\"total\":5},\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"twitter\",\"_score\":null,\"_source\":{\"id\":\"2\",\"user\":\"lopex\"},\"_type\":\"tweet\",\"sort\":[\"2\"]}],\"max_score\":null,\"total\":2},\"timed_out\":false,\"took\":1}"
--  print $ decodeEsQueryResult "_id" "{\"_shards\":{\"failed\":0,\"successful\":5,\"total\":5},\"hits\":{\"hits\":[{\"_id\":\"2\",\"_index\":\"twitter\",\"_score\":null,\"_source\":{\"id\":\"2\",\"user\":\"lopex\"},\"_type\":\"tweet\",\"sort\":[\"2\"]}],\"max_score\":null,\"total\":2},\"timed_out\":false,\"took\":1}"
--  print $ decodeEsQueryResult "id" "{\"_shards\":{\"failed\":0,\"successful\":5,\"total\":5},\"hits\":{\"hits\":[],\"max_score\":null,\"total\":0},\"timed_out\":false,\"took\":1}"
--  print $ decodeEsQueryResult "_id" "{\"_shards\":{\"failed\":0,\"successful\":5,\"total\":5},\"hits\":{\"hits\":[],\"max_score\":null,\"total\":0},\"timed_out\":false,\"took\":1}"
