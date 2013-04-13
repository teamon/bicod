{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings    #-}

-- https://github.com/selectel/mongoDB-haskell/blob/1.3.2/doc/tutorial.md
-- https://gist.github.com/teamon/56133b8a96dc1e355b2a

module Database.Bicod where


-- Base
import           Control.Applicative
import           Control.Arrow
import           Data.ByteString            (ByteString)
import qualified Data.ByteString.Lazy.Char8 as C8
import           Data.Functor
import           Data.Maybe
import           Data.Text                  hiding (count, find)
import           Debug.Trace

-- ElasticSearch
import           Data.Aeson
import           Data.Aeson.Types
import           Network.HTTP

-- MongoDB
import           Database.MongoDB           hiding (Host, Value)
import qualified Database.MongoDB           as M


dbg :: Show a => a -> a
dbg x = traceShow x x


type V a = Either String a
type Host = (String, Int) -- hostname and port
type OpsBounds = IO (V (Maybe String, Maybe String))
type OpsCount = IO (V Int)

data Sort = ASC | DESC

sortInt :: Sort -> Int
sortInt ASC   = 1
sortInt DESC  = -1

instance ToJSON Sort where
  toJSON ASC = "asc"
  toJSON DESC = "desc"

data Ops = Ops {
    opsShow   :: String
  , opsBounds :: Maybe String -> Maybe String -> OpsBounds
  , opsCount  :: Maybe String -> Maybe String -> OpsCount
}

instance Show Ops where
  show = opsShow



data ElasticSearch = ElasticSearch {
    esHost  :: Host
  , esIndex :: String
  , esType  :: String
  , esField :: String
} deriving (Show)


defaultElasticSearch :: ElasticSearch
defaultElasticSearch = ElasticSearch ("localhost", 9200) "twitter" "tweet" "id"


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
esHostString (ElasticSearch (host, port) index tp _) = host ++ ":" ++ show port

esURI :: ElasticSearch -> String
esURI es @ (ElasticSearch (host, port) index tp _) =
  "http://" ++ esHostString es ++ "/" ++ index ++ "/" ++ tp

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
  res <- simpleHTTP req

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


esOps :: ElasticSearch -> Ops
esOps es = Ops {
    opsShow = show es
  , opsCount = esCount es
  , opsBounds = esBounds es
}






-- Mongo

data Mongo = Mongo {
    mongoHost       :: Host
  , mongoDatabase   :: String
  , mongoCollection :: String
  , mongoField      :: String
} deriving (Show)

mongoHost' :: Mongo -> M.Host
mongoHost' (Mongo (host, port) _ _ _) =
  M.Host host (PortNumber $ fromInteger $ toInteger port)

defaultMongo :: Mongo
defaultMongo = Mongo ("localhost", 27017) "test" "test" "_id"

mongoCount :: Mongo -> Maybe String -> Maybe String -> OpsCount
mongoCount mongo left right = mongoRun mongo $ action where
  action = count $ (mongoQuery mongo left right ASC)

mongoBounds :: Mongo -> Maybe String -> Maybe String -> OpsBounds
mongoBounds mongo left right = (liftA2 . liftA2)(,) lx rx where
  lx = run ASC
  rx = run DESC
  run s = ((firstKey <$>) <$>) $ mongoRun mongo $ act s >>= rest
  act s = find $ (mongoQuery mongo left right s) { limit = 1}
  firstKey ds = show <$> ((listToMaybe ds) >>= M.look field)
  field = Data.Text.pack (mongoField mongo)

mongoQuery :: Mongo -> Maybe String -> Maybe String -> Sort -> Query
mongoQuery mongo lx rx sr =
  (select filt col) { project = [field =: 1], sort = [field =: sortInt sr] } where
    col     = Data.Text.pack $ mongoCollection mongo
    field   = Data.Text.pack $ mongoField mongo
    filt    = case catMaybes [fLeft, fRight] of
      [] -> []
      xs -> [field =: xs]
    fLeft   = (\x -> "$gte" =: typedField x) <$> lx
    fRight  = (\x -> "$lte" =: typedField x) <$> rx
    typedField v = read v :: ObjectId


mongoRun :: Mongo -> Action IO a -> IO (V a)
mongoRun mongo action = do
  pipe  <- runIOE $ connect $ mongoHost' mongo
  res   <- access pipe master db $ action
  return $ left show $ res where
    db = Data.Text.pack $ mongoDatabase mongo


mongoOps :: Mongo -> Ops
mongoOps mongo = Ops {
    opsShow = show mongo
  , opsCount = mongoCount mongo
  , opsBounds = mongoBounds mongo
}





opsGlobalBounds :: Ops -> OpsBounds
opsGlobalBounds ops = opsBounds ops Nothing Nothing

opsGlobalCount :: Ops -> OpsCount
opsGlobalCount ops = opsCount ops Nothing Nothing

runOps :: (Ops, Ops) -> IO ()
runOps (a, b) = do
  print a
  putStr " count:  "
  print =<< opsGlobalCount a
  putStr " bounds: "
  print =<< opsGlobalBounds a

  print b
  putStr " count:  "
  print =<< opsGlobalCount b
  putStr " bounds: "
  print =<< opsGlobalBounds b


