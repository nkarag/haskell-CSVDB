{-|
Module      : CSVdb.System.CommandLine
Description : Implements a basic command line (SQLPlus like) interface for CSVDB
Copyright   : (c) Nikos KAragiannidis, 2017
                  
License     : GPL-3
Maintainer  : nkarag@gmail.com
Stability   : experimental
Portability : POSIX

Here is a longer description of this module, containing some
commentary with @some markup@.
-}

{-# LANGUAGE OverloadedStrings #-}

module CSVdb.System.CommandLine where

import CSVdb
--import Ralgebra
import Data.RTable
import QProcessor
import System.IO
import Data.Char (toLower)

main :: IO ()
main = do
    printWelcomeNote
    mainLoop

-- | Print a welcome note.
printWelcomeNote :: IO()
printWelcomeNote = do
    putStrLn "*********************************************************************"
    putStrLn "*                                                                   *"
    putStrLn "*                  <<<< CSV DB >>>>>                     "
    putStrLn "*                                                                   *"
    putStrLn "*********************************************************************"
    putStrLn ""    
    putStrLn "Copyright Nikos Karagiannidis (c) 2017"
    putStrLn "All rights reserved."
    putStrLn ""    
    putStrLn ""   
    putStrLn ""   
    hFlush stdout

-- | Main loop
mainLoop :: IO()
mainLoop = do
    printCommands
    putStr "csvDB> "
    hFlush stdout 
    command <- getLine
    if (toLower $ head command) == 'q'
        then return ()
        else do
                let result = dispatch command
                print result
    
-- | Prints list of supported command
printCommands :: IO()
printCommands = do
                  putStrLn "SELECT Operation:     SELECT <column_list> FROM <file.csv> WHERE <predicate>"
                  putStrLn "Quit:                 q"


-- | Dispatches execution of a command
dispatch :: 
    String  -- ^ command
    -> RTable -- ^ ouput of command
dispatch cmd =
    case cmdOp (cmdRec) of 
           SelectOp -> runSelectOp $ cmdParams (cmdRec)
           OtherOp  -> emptyRTable
    where cmdRec = parseCommand $ words cmd



-------------------- NEW IDEA -------------------------------
{--
    Example:

    LOAD <csv filepath> INTO <rtab name>

    LIST <rtab name> LIMIT <num rows>

    PROJECT <rtab name> BYPOS  <list of column posistions - comma separated> INTO <rtab name>

    PROJECT <rtab name> BYNAME  <list of column Names - comma separated> INTO <rtab name>

    FILTER <rtab name> WHERE <predicate>

    <rtab name> JOIN <rtab name> ON <predicate>

    -- Command Composition

    
--}

--data Command  = System CommandSys
 --             | Etl CommandEtl


-- | Predicate data type: 
-- A Predicate data type is an expression of simple filter and/or join predicates connected by Logical Operations (AND, OR)
-- A Filter predicate in general is a function of the form (RTuple -> Bool),  and a Join Predicate in general is
-- a function of the form (RTuple -> RTuple -> Bool). With this data type we model simple predicates only, i.e., ones
-- with equality and ordering operators.
--
--  E.g., The user input predicate: 
-- @ 
-- myColumn >= 35 AND yourColumn = "Hello" AND joinColA = joinColB
-- @
--  will be trasnformed to the following Predicate data type: 
-- @
--      ("myColumn" :>=: RInt 35) :&&: ("yourColumn" :=: RText "Hello") :&&: "joinColA" :j=: "joinColB"
-- @
--
data Predicate = EmptyPred
                | PredElement :&&: Predicate
                | PredElement :||:  Predicate  
                deriving (Show, Eq)

data PredElement = ColumnName :=: RDataType
                 | ColumnName :!=: RDataType                 
                 | ColumnName :>=: RDataType
                 | ColumnName :>: RDataType
                 | ColumnName :<=: RDataType
                 | ColumnName :<: RDataType 
                 | ColumnName :j=: ColumnName
                 | ColumnName :j!=: ColumnName
                 | ColumnName :j>=: ColumnName
                 | ColumnName :j>: ColumnName
                 | ColumnName :j<=: ColumnName
                 | ColumnName :j<: ColumnName
                 deriving (Show, Eq)
