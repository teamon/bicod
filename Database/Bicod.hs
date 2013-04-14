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
import           Control.Monad.Trans        (liftIO)
import           Control.Monad.Trans.Either hiding (left)
import           Data.ByteString            (ByteString)
import qualified Data.ByteString.Lazy.Char8 as C8
import           Data.Functor
import           Data.Maybe
import           Data.Text                  as T (pack, splitOn, unpack)
import           Data.Text                  hiding (count, find, split)
import           Data.Text.Read             (decimal)
import           Data.Char
import           Data.List                  (elemIndex)
import           Data.Set (Set)
import qualified Data.Set                   as Set
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

data OpsBounds a = OpsBounds {
    boundLeft   :: Maybe a
  , boundRight  :: Maybe a
  , boundCount  :: Int
} deriving (Eq, Show)

type OpsResult a = IO (V (OpsBounds a))

data Sort = ASC | DESC

sortInt :: Sort -> Int
sortInt ASC   = 1
sortInt DESC  = -1

instance ToJSON Sort where
  toJSON ASC = "asc"
  toJSON DESC = "desc"

data Ops a = Ops {
    opsShow   :: String
  , opsBounds :: Maybe a -> Maybe a -> OpsResult a
}

instance Show (Ops a) where
  show = opsShow


class Pivot a where
  pivot :: a -> a -> a

instance Pivot String where
  pivot = pivotString c2i i2c 75 where
    c2i = (toInteger . subtract 48 . ord)
    i2c = (chr . fromIntegral . (+) 48)

instance Pivot ObjectId where
  pivot a b = read $ pivotString c2i i2c 16 (show a) (show b) where
    c2i c = toInteger $ fromMaybe 0 $ elemIndex c xs
    i2c i = xs !! (fromIntegral i)
    xs = ['0'..'9'] ++ ['a'..'f']


pivotString :: (Char -> Integer) -> (Integer -> Char) -> Integer -> (String -> String -> String)
pivotString c2i i2c base a b = i2f $ (a' + b') `div` 2 where
  a' = f2i a
  b' = f2i b
  i2f :: Integer -> String
  i2f x = fmap i2c $ Prelude.reverse $ skipZero $ f x where
    f x = if x <= 0 then [] else (x `mod` base) : f (x `div` base)
    skipZero (0:xs) = xs
    skipZero xs     = xs
  f2i :: String -> Integer
  f2i x = Prelude.foldl (\a (i,e) -> e * base^i + a) 0 $ Prelude.zip [0..] $ fmap c2i $ Prelude.reverse x



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
  esConvFrom v = fst <$> listToMaybe (reads v)


esBounds :: (EsConv a) => ElasticSearch -> Maybe a -> Maybe a -> OpsResult a
esBounds es left right = (cm <$>) <$> (liftA2 . liftA2)(,) lx rx where
  lx = query ASC
  rx = query DESC
  query = esSearchQuery es left right
  cm (EsQueryResult t lh, EsQueryResult _ rh) = OpsBounds lh rh t


esOps :: (Show a, EsConv a) => ElasticSearch -> Ops a
esOps es = Ops {
    opsShow = show es
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

mongoBounds :: (Show a, Val a) => Mongo -> Maybe a -> Maybe a -> OpsResult a
mongoBounds mongo left right = (liftA3 . liftA3) OpsBounds lx rx cn where
  lx = run ASC
  rx = run DESC
  cn = mongoRun mongo action where
    action = count $ mongoQuery mongo left right ASC
  run s = ((getField <$>) <$>) $ mongoRun mongo $ act s
  act s = findOne $ (mongoQuery mongo left right s) { limit = 1}
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


mongoRun :: Mongo -> Action IO a -> IO (V a)
mongoRun mongo action = do
  pipe  <- runIOE $ connect $ mongoHost' mongo
  res   <- access pipe master db action
  return $ left show res where
    db = Data.Text.pack $ mongoDatabase mongo


mongoOps :: (Show a, Val a) => Mongo -> Ops a
mongoOps mongo = Ops {
    opsShow = show mongo
  , opsBounds = mongoBounds mongo
}

bounds :: (Ops a, Ops a) -> Maybe a -> Maybe a -> IO (V (OpsBounds a, OpsBounds a))
bounds (a,b) lx rx = (liftA2 . liftA2)(,) a' b' where
  a' = opsBounds a lx rx
  b' = opsBounds b lx rx

globalBounds :: (Ops a, Ops a) -> IO (V (OpsBounds a, OpsBounds a))
globalBounds ops = bounds ops Nothing Nothing

findMissing :: (Pivot a, Show a, Eq a, Ord a) => (Ops a, Ops a) -> Maybe a -> Maybe a -> Int -> EitherT String IO (Set a)
findMissing (a,b) lx rx depth = do
  (ax, bx) <- EitherT $ bounds (a,b) lx rx
  --liftIO $ putStrLn $ ">>> CHECKING " ++ (show lx) ++ " - " ++ (show rx)
  --liftIO $ print ax
  --liftIO $ print bx
  case (ax, bx) of
    (ax, bx) | ax == bx ->
      return Set.empty -- liftIO $ print "OK"

    (OpsBounds al ar ac, OpsBounds bl br bc) | (ac + bc) <= 3 -> do
      liftIO $ putStrLn $ "MISSING: " ++ (show missing)
      return $ fromMaybe Set.empty $ Set.singleton <$> missing
        where
          missing = if al /= bl && ar == br       then al
                    else if al == bl && ar /= br  then ar
                    else Nothing

    (OpsBounds (Just al) (Just ar) ac, OpsBounds (Just bl) (Just br) bc) -> do
      -- left side
      --liftIO $ putStrLn $ (show depth) ++ " COUNT MISSMATCH -> LEFT " ++ (show al) ++ " - " ++ (show p)
      --liftIO $ getLine
      ls <- findMissing (a,b) (Just al) (Just p) (depth + 1)
      -- right side
      --liftIO $ putStrLn $ (show depth) ++ " COUNT MISSMATCH -> RIGHT " ++ (show p) ++ " - " ++ (show ar)
      --liftIO $ getLine
      rs <- findMissing (a,b) (Just p) (Just ar) (depth + 1)

      return $ Set.union ls rs

      where
        p = pivot al ar

    (ax, bx) -> do
      liftIO $ putStrLn "MISSMATCH"
      return Set.empty

runOps :: (Pivot a, Show a, Eq a, Ord a) => (Ops a, Ops a) -> IO ()
runOps os @ (a,b) = do
  e <- runEitherT $ findMissing (a,b) Nothing Nothing 0
  case e of
    Left err -> liftIO $ print $ "ERROR: " ++ err
    Right ms -> do
      liftIO $ putStrLn "DONE"
      liftIO $ putStrLn $ "Found " ++ (show $ Set.size ms) ++ " missing records:"
      liftIO $ printMissing ms >> return ()
      where
        printMissing ms = sequence $ (print) <$> Set.toList ms


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


