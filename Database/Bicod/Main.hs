{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings    #-}

import           Database.Bicod
import           System.Environment (getArgs)

main = getArgs >>= runBicod

