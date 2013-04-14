{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}

import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO)
import Data.Functor
import Control.Monad

import Database.Bicod
import Database.MongoDB

--newtype Alpha = Alpha { runAplha :: [Char] } deriving (Eq, Show)

--instance Arbitrary ObjectId where
    --arbitrary     = genObjectId -- liftM (Alpha . filter (`elem` ['0'..'z'])) $ arbitrary
--    --coarbitrary c = variant (ord c `rem` 4)

main = hspec $ do
  describe "Bidoc" $ do
    describe "Main" $ do
      describe "getOpsFromURI" $ do
        describe "MongoDB" $ do
          it "parse host and port" $ do
            (show <$> getOpsFromURI "mongo://localhost:555/test/posts/_id") `shouldBe` (Right $ show (Mongo ("localhost", 555) "test" "posts" "_id"))

          it "use default port" $ do
            (show <$> getOpsFromURI "mongo://localhost/test/posts/_id") `shouldBe` (Right $ show (Mongo ("localhost", 27017) "test" "posts" "_id"))


        describe "ElasticSearch" $ do
          it "parse host and port" $ do
            (show <$> getOpsFromURI "es://localhost:555/posts/post/id") `shouldBe` (Right $ show (ElasticSearch ("localhost", 555) "posts" "post" "id"))

          it "use default port" $ do
            (show <$> getOpsFromURI "es://localhost/posts/post/id") `shouldBe` (Right $ show (ElasticSearch ("localhost", 9200) "posts" "post" "id"))


      describe "Pivot" $ do
        --it "Return the same id" $ monadicIO $ do
        --  oid <- genObjectId
        --  assert $ pivot oid oid == oid

        it "should work for numbers" $ do
          pivot "123456" "543210" `shouldBe` "333333"

        it "should work for mongo oid" $ do
          pivot a b `shouldBe` c
            where
              a = read "5169926decd2f29305538415" :: ObjectId
              b = read "5169952687b1cd974758065a" :: ObjectId
              c = read "516993ca3a4260152655c537" :: ObjectId





