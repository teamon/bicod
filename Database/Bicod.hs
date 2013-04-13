{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings    #-}

-- https://github.com/selectel/mongoDB-haskell/blob/1.3.2/doc/tutorial.md
-- https://gist.github.com/teamon/56133b8a96dc1e355b2a

module Database.Bicod where

import           Control.Arrow
import           Control.Monad
import           Control.Applicative
import           Data.Maybe
import           Data.Functor
import           Data.Text      hiding (count)
import           Data.Text.Read (decimal)
import           Data.Aeson
import            Data.Aeson.Types
import qualified Data.ByteString.Lazy.Char8 as C8
import           Network.Curl   (CurlCode (CurlOK), curlGetString)
import Data.ByteString (ByteString)

import Network.HTTP

--class DbOps a where


--import Database.MongoDB

type V a = Either String a
type Host = (String, Int) -- hostname and port
type OpsBounds = IO (V (Maybe String, Maybe String))
type OpsCount = IO (V Int)

data Sort = ASC | DESC

instance ToJSON Sort where
  toJSON ASC = "asc"
  toJSON DESC = "desc"

data Ops a = Ops {
    opsBounds :: Maybe String -> Maybe String -> OpsBounds
  , opsCount :: Maybe String -> Maybe String -> OpsCount
}



data ElasticSearch = ElasticSearch {
    esHost  :: Host
  , esIndex :: String
  -- , esType  :: String
  , esField :: String
} deriving (Show)


defaultElasticSearch :: ElasticSearch
defaultElasticSearch = ElasticSearch ("localhost", 9200) "twitter" "id"


data EsQueryResult = EsQueryResult {
    esTotal :: Int
  , esHit   :: Maybe String
} deriving (Show)


decodeEsQueryResult :: String -> C8.ByteString -> V EsQueryResult
decodeEsQueryResult key json = eitherDecode json >>= parseEither parser where
  parser :: Value -> Parser EsQueryResult
  parser (Object o) =
    EsQueryResult <$>
      ((o .: "hits") >>= (.: "total")) <*>
      ((o .: "hits") >>= (.: "hits") >>= f) where
        f :: [Object] -> Data.Aeson.Types.Parser (Maybe String)
        f xs = case listToMaybe xs of
              Just o' -> (o' .: "_source") >>= (.: Data.Text.pack key)
              _       -> return Nothing

esHostString :: ElasticSearch -> String
esHostString (ElasticSearch (host, port) index _) = host ++ ":" ++ show port

esURI :: ElasticSearch -> String
esURI es @ (ElasticSearch (host, port) index _) =
  "http://" ++ esHostString es ++ "/" ++ index

esSearchQuery :: ElasticSearch -> Maybe String -> Maybe String -> Sort -> IO (V EsQueryResult)
esSearchQuery es left right sort = parse <$> httpPOST uri headers body where
  uri = esURI es ++ "/_search"
  parse res = res >>= (decodeEsQueryResult field . C8.pack)
  body = C8.unpack $ encode json

  json = object [
      "sort" .= jsSort,
      "query" .= jsQuery
    ]
  jsSort  = object [Data.Text.pack field .= sort]
  jsQuery = object ["range" .= jsRange]
  jsRange = object [Data.Text.pack field .= jsRargs]
  jsRargs = object $ catMaybes [jsLeft, jsRight]
  jsLeft  = ("gte" .=) <$> left
  jsRight = ("lte" .=) <$> right

  field = esField es
  headers = [
      mkHeader HdrHost (esHostString es),
      mkHeader HdrAccept "application/json",
      mkHeader HdrContentType "application/json; charset=utf-8",
      mkHeader HdrContentLength $ show (Prelude.length body)
    ]



httpPOST :: String -> [Header] -> String -> IO (V String)
httpPOST = httpRequest POST

httpRequest :: RequestMethod -> String -> [Header] -> String -> IO (V String)
httpRequest method uri headers body = do
  --putStrLn "-->"
  --print req
  --putStrLn $ rqBody req
  --putStrLn ""
  res <- simpleHTTP req
  --putStrLn "<--"
  --print res
  --putStrLn =<< getResponseBody res

  return $ f res where
    req = (getRequest uri) { rqMethod = method, rqBody = body, rqHeaders = headers }
    f (Left e)  = Left $ show e
    f (Right (Response code _ _ body)) = case code of
      (2, _, _) -> Right body
      _         -> Left body

esCount :: ElasticSearch -> Maybe String -> Maybe String -> OpsCount
esCount es left right = (esTotal <$>) <$> esSearchQuery es left right ASC

esBounds :: ElasticSearch -> Maybe String -> Maybe String -> OpsBounds
esBounds es left right = (cm <$>) <$> (liftA2 . liftA2)(,) lx rx where
  lx = esSearchQuery es left right ASC
  rx = esSearchQuery es left right DESC
  cm (EsQueryResult _ lh, EsQueryResult _ rh) = (lh, rh)


esOps :: ElasticSearch -> Ops ElasticSearch
esOps es = Ops {
    opsCount = esCount es
  , opsBounds = esBounds es
}






-- Mongo

data Mongo = Mongo {
    mongoHost        :: Host
  , mongoDatabase    :: Text
  , mondgoCollection :: Text
  , mongoField       :: Text
} deriving (Show)

defaultMongo :: Mongo
defaultMongo = Mongo ("localhost", 27017) "test" "test" "_id"

mongoOps :: Mongo -> Ops Mongo
mongoOps mongo = Ops {
    opsCount = undefined
  , opsBounds = undefined
}









opsGlobalBounds :: Ops a -> OpsBounds
opsGlobalBounds ops = opsBounds ops Nothing Nothing

opsGlobalCount :: Ops a -> OpsCount
opsGlobalCount ops = opsCount ops Nothing Nothing

runOps :: Ops a -> Ops b -> IO ()
runOps b a = do
  putStr "bound: "
  print =<< opsGlobalBounds b
  putStr "count: "
  print =<< opsGlobalCount b

  --putStr "bound 1 x: "
  --print =<< opsBounds b (Just "1") Nothing
  --putStr "count 1 x: "
  --print =<< opsCount b (Just "1") Nothing

  --putStr "bound 1 2: "
  --print =<< opsBounds b (Just "1") (Just "2")
  --putStr "count 1 2: "
  --print =<< opsCount b (Just "1") (Just "2")

  --putStr "bound 1 3: "
  --print =<< opsBounds b (Just "1") (Just "3")
  --putStr "count 1 3: "
  --print =<< opsCount b (Just "1") (Just "3")

  --putStr "bound x 1: "
  --print =<< opsBounds b Nothing (Just "1")
  --putStr "count x 1: "
  --print =<< opsCount b Nothing (Just "1")

  --putStr "bound x 2: "
  --print =<< opsBounds b Nothing (Just "2")
  --putStr "count x 2: "
  --print =<< opsCount b Nothing (Just "2")

  --putStr "bound 2 3: "
  --print =<< opsBounds b (Just "2") (Just "3")
  --putStr "count 2 3: "
  --print =<< opsCount b (Just "2") (Just "3")


