{-|
Module      : CSVdb.System.QProcessor
Description : Implements a basic Query Processing Engine.
Copyright   : (c) Nikos KAragiannidis, 2016
                  
License     : GPL-3
Maintainer  : nkarag@gmail.com
Stability   : experimental
Portability : POSIX

Here is a longer description of this module, containing some
commentary with @some markup@.
-}

{-# LANGUAGE OverloadedStrings #-}

module CSVdb.System.QProcessor
    ( 
        Operation (..)
        ,CommandRec (..)
        ,parseCommand
        ,runSelectOp
    ) where

--import Ralgebra 

import Data.RTable


-- | Basic operations to be supported by the Query Processor Engine
data Operation
    =   SelectOp    -- ^ A simple select operation. It returns the specified columns of the rows that satisfy the predicate.
                --  The syntax is of the form: "SELECT <column_list> FROM <file.csv> WHERE <predicate>"
    |   OtherOp     -- ^ Other operator not supported yet.


-- | This structure represents a parsed command
data CommandRec 
    =   CommandRec {
            cmdOp :: Operation  -- ^  The operation
            ,cmdParams :: OpParameters  -- ^ The operation parameters 
        }
        | EmptyCmd  -- ^ an empty command

-- | This structure represents an operations parameter
data OpParameters
    = OpParameters  {   colList :: [String] -- ^ List of columns to be returned
                        ,tupleSources :: [(String, SourceType)] -- ^ A list of the input tuple sources (e.g. a CSV file), along with its type (e.g. CSV)
                        ,predicate :: RPredicate   -- ^ The predicate to be applied at each tuple
                        -- ^ groupby
                        -- ^ having
                        -- ^ rows returned limit
                    } 


-- | Types of input data sources supported
data SourceType
    = CSV   -- ^ A csv file data source
    | Other -- ^ An unsupported data source


-- | parseCommand: Parses and input command in order to return a Command Structure
parseCommand ::
    [String]  -- ^ the command words
    -> CommandRec  -- ^ the output command structure
-- parseCommand cmd = undefined    
parseCommand ("SELECT" : cols : "FROM" : table : "WHERE" : query_pred : []) = 
--            let  pred_function x = True
--            in 
                CommandRec {    cmdOp = SelectOp, 
                                cmdParams = OpParameters { colList = words cols, tupleSources = [(table, CSV)], predicate = (\_ -> True) } 
                        }
parseCommand _ = EmptyCmd

-- | runSelectOp. Executes the SelectOp operation
runSelectOp ::
    OpParameters    -- ^ input operation parameters
    -> RTable -- ^ output result 
runSelectOp opParams = undefined




