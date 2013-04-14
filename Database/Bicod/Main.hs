{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings    #-}

import           Database.Bicod
import           System.Environment (getArgs)

main = getArgs >>= runBicod

--main = run ["mongo://localhost:27017/test/test/_id", "es://localhost:9200/twitter/tweet/id"]

