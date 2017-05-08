{-|
Module      : Ralgebra
Description : Implements basic raltional algebra operators and operations.
Copyright   : (c) Nikos KAragiannidis, 2016
                  
License     : GPL-3
Maintainer  : nkarag@gmail.com
Stability   : experimental
Portability : POSIX

Here is a longer description of this module, containing some
commentary with @some markup@.
-}

{-# LANGUAGE OverloadedStrings #-}

module Ralgebra
    ( 
       -- Relation(..)
        RTable (..)
        ,RTuple (..)
        ,RPredicate (..)
        ,ColumnName
        ,RDataType (..)
        ,ROperation (..)
    ) where


-- Vector
import qualified Data.Vector as V      

-- HashMap                          -- https://hackage.haskell.org/package/unordered-containers-0.2.7.2/docs/Data-HashMap-Strict.html
import Data.HashMap.Strict as HM


--import qualified Data.Map.Strict as Map -- Data.Map.Strict  https://www.stackage.org/haddock/lts-7.4/containers-0.5.7.1/Data-Map-Strict.html
--import qualified Data.Set as Set        -- https://www.stackage.org/haddock/lts-7.4/containers-0.5.7.1/Data-Set.html#t:Set
--import qualified Data.ByteString as BS  -- Data.ByteString  https://www.stackage.org/haddock/lts-7.4/bytestring-0.10.8.1/Data-ByteString.html

{--
-- | Definition of the Relation entity
data Relation 
--    = RelToBeDefined deriving (Show)
    =  Relation -- ^ A Relation is essentially a set of tuples
                {   
                    relname :: String  -- ^ The name of the Relation (metadata)
                    ,fields :: [RelationField]   -- ^ The list of fields (i.e., attributes) of the relation (metadata)
                    ,tuples ::   Set.Set Rtuple     -- ^ A relation is essentially a set o tuples (data)
                }
    |  EmptyRel -- ^ An empty relation
    deriving Show
--}

-- * ########## Data Types ##############

-- | Definition of the Relational Table entity
data RTable = Vector RTuple deriving (Show, Eq)


-- | Definiiton of the Relational Tuple
data RTuple = HashMap ColumnName RDataType deriving (Show, Eq)

-- | Definition of the Column Name
type ColumnName = String 

-- | Definition of the Relational Data Types
-- These will be the data types supported by the RTable
data RDataType = 
      RInteger 
    | RChar
    | RText
    | RDate 
    | RNumber
    deriving (Show, Eq)

{--
-- | Definition of a relational tuple
data Rtuple 
--    = TupToBeDefined deriving (Show)
    = Rtuple {
                fieldMap :: Map.Map RelationField Int -- ^ provides a key,value mapping  between the field and the Index (i.e., the offset) in the bytestring
                ,fieldValues :: BS.ByteString          -- ^ tuple values are stored in a bytestring
            }
    deriving Show
--}


{--

-- | Definition of a relation's field
data RelationField
    = RelationField {
                        fldname :: String
                        ,dataType :: DataType
                    }
    deriving Show                    

-- | Definition of a data type
data DataType 
    = Rinteger  -- ^ an integer data type
    | Rstring   -- ^ a string data type
    | Rdate     -- ^ a date data type
    deriving Show



-- | Definition of a predicate
type Predicate a
    =  a -> Bool    -- ^ a predicate is a polymorphic type, which is a function that evaluates an expression over an 'a' 
                    -- (e.g., a can be an Rtuple, thus Predicate Rtuple) and returns either true or false.


-- |    The selection operator.
--      It filters the tuples of a relation based on a predicate
--      and returns a new relation with the tuple that satisfy the predicate
selection :: 
        Relation  -- ^ input relation
    ->  Predicate Rtuple  -- ^ input predicate      
    ->  Relation  -- ^ output relation
selection r p = undefined  

--}

-- | Definition of a Predicate
type RPredicate = RTuple -> Bool

-- | Definition of Relational Algebra operations.
-- These are the valid operations between RTables
data ROperation = 
      RUnion
    | RIntersection
    | RDifference
    | RProjection
    | RRestriction
    | RJoin
    | RFieldRenaming
    | RAggregation
    | RExtension
