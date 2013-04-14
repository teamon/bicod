{-# LANGUAGE ExtendedDefaultRules   #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE TypeSynonymInstances   #-}
{-# LANGUAGE FlexibleInstances      #-}

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
import           Data.Text                  as T (pack, splitOn, unpack)
import           Data.Text                  hiding (count, find, split)
import           Data.Text.Read             (decimal)
import           Data.Char
import           Debug.Trace

-- ElasticSearch
import           Data.Aeson
import           Data.Aeson.Types
import           Network.HTTP

-- MongoDB
import           Database.MongoDB           hiding (Host, Value, Field)
import qualified Database.MongoDB           as M hiding (Field)


dbg :: Show a => a -> a
dbg x = traceShow x x

dbgf :: (Show a, Show b) => String -> (a -> b) -> (a -> b)
dbgf s f a = trace (s ++ " <-- " ++ show a) $ trace (s ++ " --> " ++ show b) b where
  b = f a


type V a = Either String a
type Host = (String, Int) -- hostname and port
type OpsBounds a = IO (V (Maybe a, Maybe a))
type OpsCount = IO (V Int)

data Sort = ASC | DESC

sortInt :: Sort -> Int
sortInt ASC   = 1
sortInt DESC  = -1

instance ToJSON Sort where
  toJSON ASC = "asc"
  toJSON DESC = "desc"

data Ops a = Ops {
    opsShow   :: String
  , opsBounds :: Maybe a -> Maybe a -> OpsBounds a
  , opsCount  :: Maybe a -> Maybe a -> OpsCount
}

instance Show (Ops a) where
  show = opsShow



data ElasticSearch = ElasticSearch {
    esHost  :: Host
  , esIndex :: String
  , esType  :: String
  , esField :: String
} deriving (Show)


defaultElasticSearch :: ElasticSearch
defaultElasticSearch = ElasticSearch ("localhost", 9200) "twitter" "tweet" "id"


data EsQueryResult a = EsQueryResult {
    esTotal :: Int
  , esHit   :: Maybe a
} deriving (Show)


decodeEsQueryResult :: (EsConv a) => String -> C8.ByteString -> V (EsQueryResult a)
decodeEsQueryResult key json = eitherDecode json >>= parseEither parser where
  --parser :: Value -> Parser (EsQueryResult a)
  parser (Object o) =
    EsQueryResult <$>
      ((o .: "hits") >>= (.: "total")) <*>
      ((o .: "hits") >>= (.: "hits") >>= f) where
        --f :: [Object] -> Data.Aeson.Types.Parser (Maybe a)
        f xs = case listToMaybe xs of
              Just o' -> (o' .: "_source") >>= (.: Data.Text.pack key) >>= (\m -> return $ m >>= esConvFrom)
              _       -> return Nothing

esHostString :: ElasticSearch -> String
esHostString (ElasticSearch (host, port) index tp _) = host ++ ":" ++ show port

esURI :: ElasticSearch -> String
esURI es @ (ElasticSearch (host, port) index tp _) =
  "http://" ++ esHostString es ++ "/" ++ index ++ "/" ++ tp

esSearchQuery :: (EsConv a) => ElasticSearch -> Maybe a -> Maybe a -> Sort -> IO (V (EsQueryResult a))
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
  jsLeft  = ("gte" .=) . esConvTo <$> left
  jsRight = ("lte" .=) . esConvTo <$> right

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


class EsConv a where
  esConvTo   :: a -> String
  esConvFrom :: String -> Maybe a

instance EsConv String where
  esConvTo = id
  esConvFrom = id . Just

instance EsConv ObjectId where
  esConvTo = show
  esConvFrom v = fst <$> (listToMaybe $ reads v)


esCount :: (EsConv a) => ElasticSearch -> Maybe a -> Maybe a -> OpsCount
esCount es left right = (esTotal <$>) <$> esSearchQuery es left right ASC

esBounds :: (EsConv a) => ElasticSearch -> Maybe a -> Maybe a -> OpsBounds a
esBounds es left right = (cm <$>) <$> (liftA2 . liftA2)(,) lx rx where
  lx = query ASC
  rx = query DESC
  query = esSearchQuery es left right
  cm (EsQueryResult _ lh, EsQueryResult _ rh) = (lh, rh)


esOps :: (Show a, EsConv a) => ElasticSearch -> Ops a
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

mongoCount :: (Show a, Val a) => Mongo -> Maybe a -> Maybe a -> OpsCount
mongoCount mongo left right = mongoRun mongo $ action where
  action = count $ (mongoQuery mongo left right ASC)

mongoBounds :: (Show a, Val a) => Mongo -> Maybe a -> Maybe a -> OpsBounds a
mongoBounds mongo left right = (liftA2 . liftA2)(,) lx rx where
  lx = run ASC
  rx = run DESC
  run s = ((getField <$>) <$>) $ mongoRun mongo $ act s
  act s = findOne $ (mongoQuery mongo left right s) { limit = 1}
  --firstKey :: (Show a, Read a) => [Document] -> Maybe a
  --firstKey ds = dbgf "read" read <$> firstKey' ds
  --getField :: (Val a) => Maybe Document -> Maybe a
  getField doc = doc >>= M.look field >>= cast'
  field = Data.Text.pack (mongoField mongo)

mongoQuery :: (Show a, Val a) => Mongo -> Maybe a -> Maybe a -> Sort -> Query
mongoQuery mongo lx rx sr =
  (select filt col) { project = [field =: 1], sort = [field =: sortInt sr] } where
    col     = Data.Text.pack $ mongoCollection mongo
    field   = Data.Text.pack $ mongoField mongo
    filt    = case catMaybes [fLeft, fRight] of
      [] -> []
      xs -> [field =: xs]
    fLeft   = (\x -> "$gte" =: x) <$> lx
    fRight  = (\x -> "$lte" =: x) <$> rx
    --typedField v = read (show v) :: ObjectId


mongoRun :: Mongo -> Action IO a -> IO (V a)
mongoRun mongo action = do
  pipe  <- runIOE $ connect $ mongoHost' mongo
  res   <- access pipe master db $ action
  return $ left show $ res where
    db = Data.Text.pack $ mongoDatabase mongo


mongoOps :: (Show a, Val a) => Mongo -> Ops a
mongoOps mongo = Ops {
    opsShow = show mongo
  , opsCount = mongoCount mongo
  , opsBounds = mongoBounds mongo
}

opsFieldConvBase :: Integer
opsFieldConvBase = 75

opsFieldToInteger :: String -> Integer
opsFieldToInteger x = Prelude.foldl (\a (i,e) -> e * opsFieldConvBase^i + a) 0 $ Prelude.zip [1..] $ fmap (toInteger . subtract 48 . ord) $ Prelude.reverse x

opsIntegerToField :: Integer -> String
opsIntegerToField x = fmap (chr . fromIntegral . ((+) 48)) $ Prelude.reverse $ skipZero $ f x where
  f x = if x <= 0 then [] else (x `mod` opsFieldConvBase) : f (x `div` opsFieldConvBase)
  skipZero (0:xs) = xs
  skipZero xs     = xs

opsFieldPivot :: String -> String -> String
opsFieldPivot a b = opsIntegerToField $ (a' + b') `div` 2 where
  a' = opsFieldToInteger a
  b' = opsFieldToInteger b

opsGlobalBounds :: Ops a -> OpsBounds a
opsGlobalBounds ops = opsBounds ops Nothing Nothing

opsGlobalCount :: Ops a -> OpsCount
opsGlobalCount ops = opsCount ops Nothing Nothing

runOps :: (Show a) => (Ops a, Ops a) -> IO ()
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




split :: String -> String -> [String]
split d s = fmap T.unpack $ splitOn (T.pack d) (T.pack s)

getOpsFromURI :: String -> V (Ops ObjectId)
getOpsFromURI str = case split "://" str of
  ["mongo", rest] -> mongoOps <$> parseMongoURI rest
  ["es", rest]    -> esOps    <$> parseElasticSearchURI rest
  [driver, _]     -> Left $ "Unknown driver " ++ driver
  _               -> Left $ "Invalid database URI: " ++ str


-- Example: mongo://host:port/dbname/collection/field
parseMongoURI :: String -> V Mongo
parseMongoURI uri = case parseURIParts uri of
  [host, db, coll, field] -> (\h -> Mongo h db coll field) <$> parseHost 27017 host
  _                       -> Left $ "Invalid MongoDB URI " ++ uri


-- Example: es://host:port/index/field
parseElasticSearchURI :: String -> V ElasticSearch
parseElasticSearchURI uri = case parseURIParts uri of
  [host, index, tp, field]  -> (\h -> ElasticSearch h index tp field) <$> parseHost 9200 host
  _                         -> Left $ "Invalid ElasticSearch URI " ++ uri


parseURIParts :: String -> [String]
parseURIParts = split "/"


parseHost :: Int -> String -> V (String, Int)
parseHost defaultPort str = case split ":" str of
  [host, port]  -> (,) host . fst <$> decimal (T.pack port)
  [host]        -> Right (host, defaultPort)
  _             -> Left $ "Invalid host: " ++ str


runBicod :: [String] -> IO ()
runBicod argv = either putStrLn runOps $ getOps argv where
  getOps [a, b] = liftA2 (,) (getOpsFromURI a) (getOpsFromURI b)
  getOps _ = Left "USAGE: bicod URI URI"


