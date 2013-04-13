{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}

import Database.Bicod
import Data.Text as T (pack, unpack, splitOn)
import Data.Functor
import Control.Arrow
import           Data.Text.Read             (decimal)
import System.Environment (getArgs)
import Control.Applicative



split :: String -> String -> [String]
split d s = map T.unpack $ splitOn (T.pack d) (T.pack s)

parseURI :: String -> V Ops
parseURI str = case split "://" str of
  ["mongo", rest] -> parseMongoURI rest
  ["es", rest]    -> parseElasticSearchURI rest
  [driver, _]     -> Left $ "Unknown driver " ++ driver
  _               -> Left $ "Invalid database URI: " ++ str


-- Example: mongo://host:port/dbname/collection/field
parseMongoURI :: String -> V Ops
parseMongoURI uri = case parseURIParts uri of
  [host, db, coll, field] -> (\h -> (mongoOps $ Mongo h db coll field)) <$> parseHost 27017 host
  _                       -> Left $ "Invalid MongoDB URI " ++ uri


-- Example: es://host:port/index/field
parseElasticSearchURI :: String -> V Ops
parseElasticSearchURI uri = case parseURIParts uri of
  [host, index, tp, field]  -> (\h -> esOps $ ElasticSearch h index tp field) <$> parseHost 9200 host
  _                         -> Left $ "Invalid ElasticSearch URI " ++ uri


parseURIParts :: String -> [String]
parseURIParts = split "/"


parseHost :: Int -> String -> V (String, Int)
parseHost defaultPort str = case split ":" str of
  [host, port]  -> (,) host . fst <$> decimal (T.pack port)
  [host]        -> Right (host, defaultPort)
  _             -> Left $ "Invalid host: " ++ str


run :: [String] -> IO ()
run argv = either putStrLn runOps $ getOps argv where
  getOps [a, b] = liftA2 (,) (parseURI a) (parseURI b)
  getOps _ = Left "USAGE: bicod URI URI"


main = getArgs >>= run

--main = run ["mongo://localhost:27017/test/test/_id", "es://localhost:9200/twitter/tweet/id"]

