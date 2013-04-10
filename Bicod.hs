{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}

-- https://github.com/selectel/mongoDB-haskell/blob/1.3.2/doc/tutorial.md

module Bicod where

import Data.Text
import Data.Text.Read (decimal)
import Data.Functor
import Control.Arrow


--import Database.MongoDB

type V a = Either Text a
type Host = (Text, Int) -- hostname and port

data DBConfig = Mongo {
                  mongoHost         :: Host,
                  mongoDatabase     :: Text,
                  mondgoCollection  :: Text,
                  mongoField        :: Text
                }
                | ElasticSearch {
                  esHost            :: Host,
                  esIndex           :: Text,
                  esField           :: Text
                } deriving (Show, Eq)



parseURI :: Text -> V DBConfig
parseURI str = case splitOn "://" str of
  ["mongo", rest] -> parseMongoURI rest
  ["es", rest]    -> parseElasticSearchURI rest
  [driver, _]     -> Left $ "Unknown driver " `append` driver
  _               -> Left $ "Invalid database URI: " `append` str


-- Example: mongo://host:port/dbname/collection/field
parseMongoURI :: Text -> V DBConfig
parseMongoURI uri = case parseURIParts uri of
  [host, db, coll, field] -> (\h -> Mongo h db coll field) <$> (parseHost 27017 host)
  _                       -> Left $ "Invalid MongoDB URI " `append` uri


-- Example: es://host:port/index/field
parseElasticSearchURI :: Text -> V DBConfig
parseElasticSearchURI uri = case parseURIParts uri of
  [host, index, field]    -> (\h -> ElasticSearch h index field) <$> (parseHost 9200 host)
  _                       -> Left $ "Invalid ElasticSearch URI " `append` uri


parseURIParts :: Text -> [Text]
parseURIParts = splitOn "/"


parseHost :: Int -> Text -> V (Text, Int)
parseHost defaultPort str = case splitOn ":" str of
  [host, port]  -> left pack $ (,) host . fst <$> decimal port
  [host]        -> Right (host, defaultPort)
  _             -> Left $ "Invalid host: " `append` str

--main = do
  --print $ parseURI "es://localhost:1234/posts/field"
  --print $ parseURI "mongo://localhost:1234/dbname/collection/field"






-- Unsafe
parseURI' :: Text -> DBConfig
parseURI' str = case splitOn "://" str of
  ["mongo", rest] -> parseMongoURI' rest

parseMongoURI' :: Text -> DBConfig
parseMongoURI' uri = case parseURIParts' uri of
  [host, db, coll, field] -> Mongo (parseHost' 27017 host) db coll field

parseURIParts' :: Text -> [Text]
parseURIParts' = splitOn "/"

parseHost' :: Int -> Text -> (Text, Int)
parseHost' defaultPort str = case splitOn ":" str of
  [host, port]  -> (host, read $ unpack port)
  [host]        -> (host, defaultPort)



