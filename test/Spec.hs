{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}

import Test.Hspec

import Database.Bicod

main = hspec $ do
  describe "Bidoc" $ do
    describe "parseURI" $ do
      describe "MongoDB" $ do
        it "parse host and port" $ do
          parseURI "mongo://localhost:555/test/posts/_id" `shouldBe` Right (Mongo ("localhost", 555) "test" "posts" "_id")

        it "use default port" $ do
          parseURI "mongo://localhost/test/posts/_id" `shouldBe` Right (Mongo ("localhost", 27017) "test" "posts" "_id")


      describe "ElasticSearch" $ do
        it "parse host and port" $ do
          parseURI "es://localhost:555/posts/id" `shouldBe` Right (ElasticSearch ("localhost", 555) "posts" "id")

        it "use default port" $ do
          parseURI "es://localhost/posts/id" `shouldBe` Right (ElasticSearch ("localhost", 9200) "posts" "id")


      describe "mongo unsafe" $ do
        it "parse host and port" $ do
          parseURI' "mongo://localhost:555/test/posts/_id" `shouldBe` (Mongo ("localhost", 555) "test" "posts" "_id")

        it "use default port" $ do
          parseURI' "mongo://localhost/test/posts/_id" `shouldBe` (Mongo ("localhost", 27017) "test" "posts" "_id")
