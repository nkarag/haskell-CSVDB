{-|
Module      : RTable
Description : Implements the relational Table concept. Defines all necessary data types like RTable and RTuple and basic relational algebra operations on RTables.
Copyright   : (c) Nikos Karagiannidis, 2017
                  
License     : GPL-3
Maintainer  : nkarag@gmail.com
Stability   : experimental
Portability : POSIX

Here is a longer description of this module, containing some
commentary with @some markup@.
-}

{-# LANGUAGE OverloadedStrings #-}
-- :set -XOverloadedStrings
--  :set -XRecordWildCards
{-# LANGUAGE GeneralizedNewtypeDeriving  -- In order to be able to derive from non-standard derivable classes (such as Num)
            ,BangPatterns
            ,RecordWildCards 
            ,DeriveGeneric       -- Allow automatic deriving of instances for the Generic typeclass  (see Text.PrettyPrint.Tabulate.Example)
            ,DeriveDataTypeable  -- Enable automatic deriving of instances for the Data typeclass    (see Text.PrettyPrint.Tabulate.Example)
            {--
                :set -XDeriveGeneric
                :set -XDeriveDataTypeable
            --}
            -- Allow definition of type class instances for type synonyms. (used for RTuple instance of Tabulate)
            --,TypeSynonymInstances  
            --,FlexibleInstances
#-}  

-- {-# LANGUAGE  DuplicateRecordFields #-}


module Data.RTable
    ( 
       -- Relation(..)
        RTable (..)
        ,RTuple (..)
        ,RTupleFormat (..)
        ,ColFormatMap
        ,FormatSpecifier (..)
        ,RPredicate
        ,RGroupPredicate
        ,RJoinPredicate
        ,UnaryRTableOperation
        ,BinaryRTableOperation
        ,Name
        ,ColumnName
        ,RTableName
        ,ColumnDType (..)
        ,RTableMData (..)
        ,RTupleMData
        ,RTimestamp (..)
        ,ColumnInfo (..)
        ,RDataType (..)
        ,ROperation (..)
        ,RAggOperation (..)
        ,IgnoreDefault (..)
        ,RTuplesRet
        ,RTabResult
        ,OrderingSpec (..)
        ,raggSum
        ,raggCount
        ,raggAvg
        ,raggMax
        ,raggMin
        ,emptyRTable
        ,emptyRTuple
        ,isRTabEmpty
        ,isRTupEmpty
        ,createSingletonRTable
        ,getColumnNamesfromRTab
        ,getColumnNamesfromRTuple
        ,createRDataType
        ,createNullRTuple
        ,createRtuple
        ,isNullRTuple
        ,restrictNrows
        ,createRTableMData
        ,createRTimeStamp
        ,getRTupColValue        
        ,rtupLookup
        ,rtupLookupDefault
        , (<!>)
        ,headRTup
        ,upsertRTuple
        ,rTimeStampToRText
        ,stdTimestampFormat
        ,stdDateFormat
        ,listOfColInfoRDataType
        ,toListColumnName
        ,toListColumnInfo
        ,toListRDataType
        ,rtupleToList
        ,rtupleFromList
        ,rtableToList
        ,rtableFromList
        --,runRfilter
        ,f
        --,runInnerJoin
        ,iJ
        --,runLeftJoin
        ,lJ
        --,runRightJoin
        ,rJ
        -- full outer join
        ,foJ
      --  ,runUnaryROperation
        ,ropU
      --  ,runBinaryROperation
        ,ropB  
        --,runUnion
        ,u
        --,runIntersect
        ,i
        --,runDiff
        ,d 
        --,runProjection
        ,p   
        --,runAggregation
        ,rAgg
        --,runGroupBy
        ,rG 
        ,rO
        --,runCombinedROp 
        ,rComb
        ,stripRText
        ,removeCharAroundRText
        ,removeColumn
        ,addColumn
        ,isNull
        ,isNotNull
        ,nvl
        ,nvlColValue
        ,nvlRTuple
        ,nvlRTable
        ,decodeColValue
        ,decodeRTable
        ,rtabResult
        ,runRTabResult
        ,execRTabResult
        ,rtuplesRet
        ,getRTuplesRet
        ,printRTable
        ,printfRTable
        ,rtupleToList
        ,joinRTuples
        ,insertPrependRTab
        ,insertAppendRTab
        ,updateRTab
        ,rtabFoldr'
        ,rtabFoldl'
        ,rtabMap
        ,genDefaultColFormatMap
        ,genColFormatMap
        ,genRTupleFormat
        ,genRTupleFormatDefault
    ) where

import Debug.Trace

-- Data.Serialize (Cereal package)  
--                                  https://hackage.haskell.org/package/cereal
--                                  https://hackage.haskell.org/package/cereal-0.5.4.0/docs/Data-Serialize.html
--                                  http://stackoverflow.com/questions/2283119/how-to-convert-a-integer-to-a-bytestring-in-haskell
import Data.Serialize (decode, encode)

-- Vector
import qualified Data.Vector as V      

-- HashMap                          -- https://hackage.haskell.org/package/unordered-containers-0.2.7.2/docs/Data-HashMap-Strict.html
import Data.HashMap.Strict as HM

-- Text
import Data.Text as T

-- ByteString
import qualified Data.ByteString as BS

-- Typepable                        -- https://hackage.haskell.org/package/base-4.9.1.0/docs/Data-Typeable.html
                                    -- http://stackoverflow.com/questions/6600380/what-is-haskells-data-typeable
                                    -- http://alvinalexander.com/source-code/haskell/how-determine-type-object-haskell-program
import qualified Data.Typeable as TB --(typeOf, Typeable)

-- Dynamic
import qualified Data.Dynamic as D  -- https://hackage.haskell.org/package/base-4.9.1.0/docs/Data-Dynamic.html

-- Data.List
import Data.List (last, all, elem, map, zip, zipWith, elemIndex, sortOn, union, intersect, (\\), take, length, repeat, groupBy, sort, sortBy, foldl', foldr, foldr1, foldl',head)
-- Data.Maybe
import Data.Maybe (fromJust)
-- Data.Char
import Data.Char (toUpper,digitToInt, isDigit)
-- Data.Monoid
import Data.Monoid as M
-- Control.Monad.Trans.Writer.Strict
import Control.Monad.Trans.Writer.Strict (Writer, writer, runWriter, execWriter)
-- Text.Printf
import Text.Printf (printf)

-- import Control.Monad.IO.Class (liftIO)


{--- Text.PrettyPrint.Tabulate
import qualified Text.PrettyPrint.Tabulate as PP
import qualified GHC.Generics as G
import Data.Data
-}
--import qualified Text.PrettyPrint.Boxes as BX
--import Data.Map (fromList)


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
--   An RTable is essentially a vector of RTuples.
type RTable = V.Vector RTuple 


-- | Definition of the Relational Tuple
--   An RTuple is implemented as a hash map (colummname, RDataType). This ensures fast access of the column value by column name.
--   Note that this implies that the RTuple CANNOT have more than one columns with the same name (i.e. hashmap key) and more importantly that
--   it DOES NOT have a fixed order of columns, as it is usual in RDBMS implementations.
--   This gives us the freedom to perform column changes operations very fast.
--   The only place were we need fixed column order is when we try to load an RTable from a fixed-column structure such as a CSV file.
--   For this reason, we have embedded the notion of a fixed column-order in the RTuple metadata. See 'RTupleMData'.
type RTuple = HM.HashMap ColumnName RDataType

-- | Turns a RTable to a list
rtableToList :: RTable -> [RTuple]
rtableToList = V.toList

-- | Creates an RTable from a list of RTuples
rtableFromList :: [RTuple] -> RTable
rtableFromList = V.fromList

-- | Turns an RTuple to a List
rtupleToList :: RTuple -> [(ColumnName, RDataType)]
rtupleToList = HM.toList

-- | Create an RTuple from a list
rtupleFromList :: [(ColumnName, RDataType)] -> RTuple 
rtupleFromList = HM.fromList

{-
instance Data RTuple
instance G.Generic RTuple
instance PP.Tabulate RTuple
-}

-- | Definitioin of the Name type
type Name = String


-- | Definition of the Column Name
type ColumnName = Name

-- instance PP.Tabulate ColumnName

-- | Definition of the Table Name
type RTableName = Name

-- | This is used only for metadata purposes. The actual data type of a value is an RDataType
-- The String component of Date data constructor is the date format e.g., "DD/MM/YYYY"
data ColumnDType = Integer | Varchar | Date String | Timestamp String | Double deriving (Show, Eq)

-- | Definition of the Relational Data Types
-- These will be the data types supported by the RTable.
-- Essentially an RDataType is a wrpapper of Haskell common data types.
data RDataType = 
      RInt { rint :: Integer }
    -- RChar { rchar :: Char }
      | RText { rtext :: T.Text }
    -- RString {rstring :: [Char]}
      | RDate { 
                rdate :: T.Text
               ,dtformat :: String  -- ^ e.g., "DD/MM/YYYY"
            }
      | RTime { rtime :: RTimestamp  }
      | RDouble { rdouble :: Double }
    -- RFloat  { rfloat :: Float }
      | Null
      deriving (Show,TB.Typeable, Read)   -- http://stackoverflow.com/questions/6600380/what-is-haskells-data-typeable


-- | We need to explicitly specify due to NULL logic (anything compared to NULL returns false)
-- @
-- Null == _ = False
-- _ == Null = False
-- Null /= _ = False
-- _ /= Null = False
-- @
-- IMPORTANT NOTE:
--  Of course this means that anywhere in your code where you have something like this x == Null or x /= Null, will always return False and
-- thus it is futile to do this comparison. You have to use the is isNull function instead.
--
instance Eq RDataType where
    RInt i1 == RInt i2 = i1 == i2
    -- RInt i == _ = False
    RText t1 == RText t2 = t1 == t2
    -- RText t1 == _ = False
    RDate t1 s1 == RDate t2 s2 = (t1 == t1) && (s1 == s2)
    -- RDate t1 s1 == _ = False
    RTime t1 == RTime t2 = t1 == t2
    -- RTime t1 == _ = False
    RDouble d1 == RDouble d2 = d1 == d2
    -- RDouble d1 == _ = False
    -- Watch out: NULL logic (anything compared to NULL returns false)
    Null == Null = False
    _ == Null = False
    Null == _ = False
    -- anything else is just False
    _ == _ = False

    Null /= Null = False
    _ /= Null = False
    Null /= _ = False
    x /= y = not (x == y) 

-- Need to explicitly specify due to "Null logic" (see Eq)
instance Ord RDataType where
    Null <= _ = False
    _ <= Null = False
    Null <= Null = False
    RInt i1 <= RInt i2 = i1 <= i2
    RText t1 <= RText t2 = t1 <= t2
    RDate t1 s1 <= RDate t2 s2 = (t1 <= t1) && (s1 == s2)
    RTime t1 <= RTime t2 = t1 <= t2
    RTime t1 <= _ = False
    RDouble d1 <= RDouble d2 = d1 <= d2
    -- anything else is just False
    _ <= _ = False


-- | Use this function to compare an RDataType with the Null value because due to Null logic
--  x == Null or x /= Null, will always return False.
-- It returns True if input value is Null
isNull :: RDataType -> Bool
isNull x = 
    case x of
        Null -> True
        _    -> False

-- | Use this function to compare an RDataType with the Null value because deu to Null logic
--  x == Null or x /= Null, will always return False.
-- It returns True if input value is Not Null
isNotNull = not . isNull

instance Num RDataType where
    (+) (RInt i1) (RInt i2) = RInt (i1 + i2)
    (+) (RDouble d1) (RDouble d2) = RDouble (d1 + d2)
    (+) (RDouble d1) (RInt i2) = RDouble (d1 + fromIntegral i2)
    (+) (RInt i1) (RDouble d2) = RDouble (fromIntegral i1 + d2)
    -- (+) (RInt i1) (Null) = RInt i1  -- ignore Null - just like in SQL
    -- (+) (Null) (RInt i2) = RInt i2  -- ignore Null - just like in SQL
    -- (+) (RDouble d1) (Null) = RDouble d1  -- ignore Null - just like in SQL
    -- (+) (Null) (RDouble d2) = RDouble d2  -- ignore Null - just like in SQL    
    (+) (RInt i1) (Null) = Null
    (+) (Null) (RInt i2) = Null
    (+) (RDouble d1) (Null) = Null
    (+) (Null) (RDouble d2) = Null
    (+) _ _ = Null
    (*) (RInt i1) (RInt i2) = RInt (i1 * i2)
    (*) (RDouble d1) (RDouble d2) = RDouble (d1 * d2)
    (*) (RDouble d1) (RInt i2) = RDouble (d1 * fromIntegral i2)
    (*) (RInt i1) (RDouble d2) = RDouble (fromIntegral i1 * d2)
    -- (*) (RInt i1) (Null) = RInt i1  -- ignore Null - just like in SQL
    -- (*) (Null) (RInt i2) = RInt i2  -- ignore Null - just like in SQL
    -- (*) (RDouble d1) (Null) = RDouble d1  -- ignore Null - just like in SQL
    -- (*) (Null) (RDouble d2) = RDouble d2  -- ignore Null - just like in SQL
    (*) (RInt i1) (Null) = Null
    (*) (Null) (RInt i2) = Null
    (*) (RDouble d1) (Null) = Null
    (*) (Null) (RDouble d2) = Null
    (*) _ _ = Null
    abs (RInt i) = RInt (abs i)
    abs (RDouble i) = RDouble (abs i)
    abs _ = Null
    signum (RInt i) = RInt (signum i)
    signum (RDouble i) = RDouble (signum i)
    signum _ = Null
    fromInteger i = RInt i
    negate (RInt i) = RInt (negate i)
    negate (RDouble i) = RDouble (negate i)
    negate _ = Null

-- | In order to be able to use (/) with RDataType
instance Fractional RDataType where
    (/) (RInt i1) (RInt i2) = RDouble $ (fromIntegral i1)/(fromIntegral i2)
    (/) (RDouble d1) (RInt i2) = RDouble $ (d1)/(fromIntegral i2)
    (/) (RInt i1) (RDouble d2) = RDouble $ (fromIntegral i1)/(d2)
    (/) (RDouble d1) (RDouble d2) = RDouble $ d1/d2
    (/) _ _ = Null

    -- In order to be able to turn a Rational number into an RDataType, e.g. in the case: totamnt / 12.0
    -- where totamnt = RDouble amnt
    -- fromRational :: Rational -> a
    fromRational r = RDouble (fromRational r)

-- | Standard date format
stdDateFormat = "DD/MM/YYYY"

-- | Get the Column Names of an RTable
getColumnNamesfromRTab :: RTable -> [ColumnName]
getColumnNamesfromRTab rtab = getColumnNamesfromRTuple $ headRTup rtab

-- | Get the first RTuple from an RTable
headRTup ::
        RTable
    ->  RTuple
headRTup = V.head    

-- | Returns the Column Names of an RTuple
getColumnNamesfromRTuple :: RTuple -> [ColumnName]
getColumnNamesfromRTuple t = HM.keys t

-- | Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns Nothing
rtupLookup ::
       ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> Maybe RDataType     -- ^ Output value
rtupLookup =  HM.lookup

-- | Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns a default value
rtupLookupDefault ::
       RDataType     -- ^ Default value to return in the case the column name does not exist in the RTuple
    -> ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> RDataType     -- ^ Output value
rtupLookupDefault =  HM.lookupDefault


-- | getRTupColValue :: Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns Null.
--   !!!Note that this might be confusing since there might be an existing column name with a Null value!!!
getRTupColValue ::
       ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> RDataType     -- ^ Output value
getRTupColValue =  rtupLookupDefault Null  -- HM.lookupDefault Null

-- | Operator for getting a column value from an RTuple
--   Calls error if this map contains no mapping for the key.
(<!>) ::
       RTuple        -- ^ Input RTuple
    -> ColumnName    -- ^ ColumnName key
    -> RDataType     -- ^ Output value
(<!>) t c = -- flip getRTupColValue
       -- (HM.!)
       case rtupLookup c t of
            Just v -> v
            Nothing -> error $ "*** Error in function Data.RTable.(<!>): Column \"" ++ c ++ "\" does not exist! ***" 


-- | Returns the 1st parameter if this is not Null, otherwise it returns the 2nd. 
nvl ::
       RDataType  -- ^ input value
    -> RDataType  -- ^ default value returned if input value is Null
    -> RDataType  -- ^ output value
nvl v defaultVal = 
    if isNull v
        then defaultVal
        else v

-- | Returns the value of a specific column (specified by name) if this is not Null. 
-- If this value is Null, then it returns the 2nd parameter.
-- If you pass an empty RTuple, then it returns Null.
-- Calls error if this map contains no mapping for the key.
nvlColValue ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ value returned if original value is Null
    ->  RTuple      -- ^ input RTuple
    ->  RDataType   -- ^ output value
nvlColValue col defaultVal tup = 
    if isRTupEmpty tup
        then Null
        else 
            case tup <!> col of
                Null   -> defaultVal
                val    -> val 

data IgnoreDefault = Ignore | NotIgnore deriving (Eq, Show)

-- | It receives an RTuple and lookups the value at a specfic column name.
-- Then it compares this value with the specified search value. If it is eaqual to the search value
-- then it returns the specified Return Value. If not, the it returns the specified default Value (if the ignore indicator is not set).
-- If you pass an empty RTuple, then it returns Null.
-- Calls error if this map contains no mapping for the key.
decodeColValue ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ Search value
    ->  RDataType   -- ^ Return value
    ->  RDataType   -- ^ Default value   
    ->  IgnoreDefault -- ^ Ignore default indicator     
    ->  RTuple      -- ^ input RTuple
    ->  RDataType
decodeColValue cname searchVal returnVal defaultVal ignoreInd tup = 
    if isRTupEmpty tup
        then Null
        else 
            case tup <!> cname of
                searchVal   -> returnVal
                v           -> if ignoreInd == Ignore then v else defaultVal 


-- | It receives an RTuple and a default value. It returns a new RTuple which is identical to the source one
-- but every Null value in the specified colummn has been replaced by a default value
nvlRTuple ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ Default value in the case of Null column values
    ->  RTuple      -- ^ input RTuple    
    ->  RTuple      -- ^ output RTuple
nvlRTuple c defaultVal tup  = 
    if isRTupEmpty tup
       then emptyRTuple
       else HM.map (\v -> nvl v defaultVal) tup


-- | It receives an RTable and a default value. It returns a new RTable which is identical to the source one
-- but for each RTuple, for the specified column every Null value in every RTuple has been replaced by a default value
-- If you pass an empty RTable, then it returns an empty RTable
-- Calls error if the column does not exist
nvlRTable ::
        ColumnName  -- ^ ColumnName key
    ->  RDataType -- ^ Default value        
    ->  RTable    -- ^ input RTable
    ->  RTable
nvlRTable c defaultVal tab  = 
    if isRTabEmpty tab
        then emptyRTable
        else
            V.map (\t -> upsertRTuple c (nvlColValue c defaultVal t) t) tab
            --V.map (\t -> nvlRTuple c defaultVal t) tab    

-- | It receives an RTable, a search value and a default value. It returns a new RTable which is identical to the source one
-- but for each RTuple, for the specified column:
--   * if the search value was found then the specified Return Value is returned
--   * else the default value is returned  (if the ignore indicator is not set)  
-- If you pass an empty RTable, then it returns an empty RTable
-- Calls error if the column does not exist
decodeRTable ::            
        ColumnName  -- ^ ColumnName key
    ->  RDataType   -- ^ Search value
    ->  RDataType   -- ^ Return value
    ->  RDataType   -- ^ Default value        
    ->  IgnoreDefault -- ^ Ignore default indicator     
    ->  RTable      -- ^ input RTable
    ->  RTable
decodeRTable cName searchVal returnVal defaultVal ignoreInd tab = 
    if isRTabEmpty tab
        then emptyRTable
        else
            V.map (\t -> upsertRTuple cName (decodeColValue cName searchVal returnVal defaultVal ignoreInd t) t) tab   

-- | Upsert (update/insert) an RTuple at a specific column specified by name with a value
-- If the cname key is not found then the (columnName, value) pair is inserted. If it exists
-- then the value is updated with the input value.
upsertRTuple ::
           ColumnName  -- ^ key where the upset will take place
        -> RDataType   -- ^ new value
        -> RTuple      -- ^ input RTuple
        -> RTuple      -- ^ output RTuple
upsertRTuple cname newVal tupsrc = HM.insert cname newVal tupsrc

-- newtype NumericRDT = NumericRDT { getRDataType :: RDataType } deriving (Eq, Ord, Read, Show, Num)


-- | stripRText : O(n) Remove leading and trailing white space from a string.
-- If the input RDataType is not an RText, then Null is returned
stripRText :: 
           RDataType  -- ^ input string
        -> RDataType
stripRText (RText t) = RText $ T.strip t
stripRText _ = Null


-- | Helper function to remove a character around (from both beginning and end) of an (RText t) value
removeCharAroundRText :: Char -> RDataType -> RDataType
removeCharAroundRText ch (RText t) = RText $ T.dropAround (\c -> c == ch) t
removeCharAroundRText ch _ = Null

-- | RTimestamp data type
data RTimestamp = RTimestampVal {
            year :: Int
            ,month :: Int
            ,day :: Int
            ,hours24 :: Int
            ,minutes :: Int
            ,seconds :: Int
        } deriving (Show, Read)

instance Eq RTimestamp where
        RTimestampVal y1 m1 d1 h1 mi1 s1 ==  RTimestampVal y2 m2 d2 h2 mi2 s2 = 
            y1 == y2 && m1 == m2 && d1 == d2 && h1 == h2 && mi1 == mi2 && s1 == s2

instance Ord RTimestamp where
    -- compare :: a -> a -> Ordering
    compare (RTimestampVal y1 m1 d1 h1 mi1 s1) (RTimestampVal y2 m2 d2 h2 mi2 s2) = 
        if compare y1 y2 /= EQ 
            then compare y1 y2
            else 
                if compare m1 m2 /= EQ 
                    then compare m1 m2
                    else if compare d1 d2 /= EQ
                            then compare d1 d2
                            else if compare h1 h2 /= EQ
                                    then compare h1 h2
                                    else if compare mi1 mi2 /= EQ
                                            then compare mi1 mi2
                                            else if compare s1 s2 /= EQ
                                                    then compare s1 s2
                                                    else EQ

-- | Creates an RTimestamp data type from an input timestamp format string and a timestamp value represented as Text.
createRTimeStamp :: 
    String      -- ^ Format string e.g., "DD/MM/YYYY HH24:MI:SS"
    -> String   -- ^ Timestamp string
    -> RTimestamp
createRTimeStamp fmt timeVal = 
    case Prelude.map (Data.Char.toUpper) fmt of
        "DD/MM/YYYY HH24:MI:SS"     -> parseTime timeVal
        "\"DD/MM/YYYY HH24:MI:SS\"" -> parseTime timeVal
    where
        parseTime :: String -> RTimestamp
        parseTime (d1:d2:'/':m1:m2:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) * 10 + (digitToInt m2)
                                                                                                ,day = (digitToInt d1) * 10 + (digitToInt d2)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)   
                                                                                                }                                                                                                                                 
        parseTime (d1:'/':m1:m2:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) * 10 + (digitToInt m2)
                                                                                                ,day = (digitToInt d1)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)                                                                                                                                    
                                                                                               }
        parseTime (d1:'/':m1:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1) 
                                                                                                ,day = (digitToInt d1) 
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)
                                                                                               }
        parseTime (d1:d2:'/':m1:'/':y1:y2:y3:y4:' ':h1:h2:':':mi1:mi2:':':s1:s2:_) = RTimestampVal {    
                                                                                                year = (digitToInt y1) * 1000 + (digitToInt y2) * 100 + (digitToInt y3) * 10 + (digitToInt y4)
                                                                                                ,month = (digitToInt m1)
                                                                                                ,day = (digitToInt d1) * 10 + (digitToInt d2)
                                                                                                ,hours24 = (digitToInt h1) * 10 + (digitToInt h2)
                                                                                                ,minutes = (digitToInt mi1) * 10 + (digitToInt mi2)
                                                                                                ,seconds = (digitToInt s1) * 10 + (digitToInt s2)                                                                                                                                    
                                                                                                }
        parseTime _ = RTimestampVal {year = 2999, month = 12, day = 31, hours24 = 11, minutes = 59, seconds = 59}

-- | Standard timestamp format
stdTimestampFormat = "DD/MM/YYYY HH24:MI:SS"

-- | rTimeStampToText: converts an RTimestamp value to RText
rTimeStampToRText :: 
    String  -- ^ Output format e.g., DD/MM/YYYY HH24:MI:SS
    -> RTimestamp -- ^ Input RTimestamp 
    -> RDataType  -- ^ Output RText
rTimeStampToRText "DD/MM/YYYY HH24:MI:SS" ts =  let -- timeString = show (day ts) ++ "/" ++ show (month ts) ++ "/" ++ show (year ts) ++ " " ++ show (hours24 ts) ++ ":" ++ show (minutes ts) ++ ":" ++ show (seconds ts)
                                                    timeString = expand (day ts) ++ "/" ++ expand (month ts) ++ "/" ++ expand (year ts) ++ " " ++ expand (hours24 ts) ++ ":" ++ expand (minutes ts) ++ ":" ++ expand (seconds ts)
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString
rTimeStampToRText "YYYYMMDD-HH24.MI.SS" ts =    let -- timeString = show (year ts) ++ show (month ts) ++ show (day ts) ++ "-" ++ show (hours24 ts) ++ "." ++ show (minutes ts) ++ "." ++ show (seconds ts)
                                                    timeString = expand (year ts) ++ expand (month ts) ++ expand (day ts) ++ "-" ++ expand (hours24 ts) ++ "." ++ expand (minutes ts) ++ "." ++ expand (seconds ts)
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                    {-
                                                    !dummy1 = trace ("expand (year ts) : " ++ expand (year ts)) True
                                                    !dummy2 = trace ("expand (month ts) : " ++ expand (month ts)) True
                                                    !dummy3 =  trace ("expand (day ts) : " ++ expand (day ts)) True
                                                    !dummy4 = trace ("expand (hours24 ts) : " ++ expand (hours24 ts)) True
                                                    !dummy5 = trace ("expand (minutes ts) : " ++ expand (minutes ts)) True
                                                    !dummy6 = trace ("expand (seconds ts) : " ++ expand (seconds ts)) True-}
                                                in RText $ T.pack timeString
rTimeStampToRText "YYYYMMDD" ts =               let 
                                                    timeString = expand (year ts) ++ expand (month ts) ++ expand (day ts) 
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString
rTimeStampToRText "YYYYMM" ts =                 let 
                                                    timeString = expand (year ts) ++ expand (month ts) 
                                                    expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString
rTimeStampToRText "YYYY" ts =                   let 
                                                    timeString = show $ year ts -- expand (year ts)
                                                    -- expand i = if i < 10 then "0"++ (show i) else show i
                                                in RText $ T.pack timeString



-- | Metadata for an RTable
data RTableMData =  RTableMData {
                        rtname :: RTableName
                        ,rtuplemdata :: RTupleMData  -- ^ Tuple-level metadata                    
                        -- other metadata
                        ,pkColumns :: [ColumnName] -- ^ Primary Key
                        ,uniqueKeys :: [[ColumnName]] -- ^ List of unique keys i.e., each sublist is a unique key column combination
                    } deriving (Show, Eq)


-- | createRTableMData : creates RTableMData from input given in the form of a list
--   We assume that the column order of the input list defines the fixed column order of the RTuple.
createRTableMData ::
        (RTableName, [(ColumnName, ColumnDType)])
        -> [ColumnName]     -- ^ Primary Key. [] if no PK exists
        -> [[ColumnName]]   -- ^ list of unique keys. [] if no unique keys exists
        -> RTableMData
createRTableMData (n, cdts) pk uks = 
        RTableMData { rtname = n, rtuplemdata = createRTupleMdata cdts, pkColumns = pk, uniqueKeys = uks }


-- | createRTupleMdata : Creates an RTupleMData instance based on a list of (Column name, Column Data type) pairs.
-- The order in the input list defines the fixed column order of the RTuple
createRTupleMdata ::  [(ColumnName, ColumnDType)] -> RTupleMData
-- createRTupleMdata clist = Prelude.map (\(n,t) -> (n, ColumnInfo{name = n, colorder = fromJust (elemIndex (n,t) clist),  dtype = t })) clist 
--HM.fromList $ Prelude.map (\(n,t) -> (n, ColumnInfo{name = n, colorder = fromJust (elemIndex (n,t) clist),  dtype = t })) clist
createRTupleMdata clist = 
    let colNamecolInfo = Prelude.map (\(n,t) -> (n, ColumnInfo{    name = n 
                                                                    --,colorder = fromJust (elemIndex (n,t) clist)
                                                                    ,dtype = t 
                                                                })) clist
        colOrdercolName = Prelude.map (\(n,t) -> (fromJust (elemIndex (n,t) clist), n)) clist
    in (HM.fromList colOrdercolName, HM.fromList colNamecolInfo)




-- Old - Obsolete:
-- Basic Metadata of an RTuple
-- Initially design with a HashMap, but HashMaps dont guarantee a specific ordering when turned into a list.
-- We implement the fixed column order logic for an RTuple, only at metadata level and not at the RTuple implementation, which is a HashMap (see @ RTuple)
-- So the fixed order of this list equals the fixed column order of the RTuple.
--type RTupleMData =  [(ColumnName, ColumnInfo)] -- HM.HashMap ColumnName ColumnInfo         




-- | Basic Metadata of an RTuple.
--   The RTuple metadata are accessed through a HashMap ColumnName ColumnInfo  structure. I.e., for each column of the RTuple,
--   we access the ColumnInfo structure to get Column-level metadata. This access is achieved by ColumnName.
--   However, in order to provide the "impression" of a fixed column order per tuple (see 'RTuple' definition), we provide another HashMap,
--   the HashMap ColumnOrder ColumnName. So if we want to access the RTupleMData tupmdata ColumnInfo by column order, we have to do the following:
--    
-- @
--      (snd tupmdata)!((fst tupmdata)!0)
--      (snd tupmdata)!((fst tupmdata)!1)
--      ...
--      (snd tupmdata)!((fst tupmdata)!N)
-- @
--
--  In the same manner in order to access the column of an RTuple (e.g., tup) by column order we do the following:
--
-- @
--      tup!((fst tupmdata)!0)
--      tup!((fst tupmdata)!1)
--      ...
--      tup!((fst tupmdata)!N)
-- @
-- 
type RTupleMData =  (HM.HashMap ColumnOrder ColumnName, HM.HashMap ColumnName ColumnInfo) 

type ColumnOrder = Int 

-- | toListColumnName: returns a list of RTuple column names, in the fixed column order of the RTuple.
toListColumnName :: 
    RTupleMData
    -> [ColumnName]
toListColumnName rtupmd = 
    let mapColOrdColName = fst rtupmd
        listColOrdColName = HM.toList mapColOrdColName  -- generate a list of [ColumnOrdr, ColumnName] in random order
        -- order list based on ColumnOrder
        ordlistColOrdColName = sortOn (\(o,c) -> o) listColOrdColName   -- Data.List.sortOn :: Ord b => (a -> b) -> [a] -> [a]. Sort a list by comparing the results of a key function applied to each element. 
    in Prelude.map (snd) ordlistColOrdColName

-- | toListColumnInfo: returns a list of RTuple columnInfo, in the fixed column order of the RTuple
toListColumnInfo :: 
    RTupleMData
    -> [ColumnInfo]
toListColumnInfo rtupmd = 
    let mapColNameColInfo = snd rtupmd
    in Prelude.map (\cname -> mapColNameColInfo HM.! cname) (toListColumnName rtupmd)


-- | toListRDataType: returns a list of RDataType values of an RTuple, in the fixed column order of the RTuple
toListRDataType :: 
    RTupleMData
    -> RTuple
    -> [RDataType]
toListRDataType rtupmd rtup = Prelude.map (\cname -> rtup <!> cname) (toListColumnName rtupmd)


-- | Basic metadata for a column of an RTuple
data ColumnInfo =   ColumnInfo { 
                        name :: ColumnName  
                      --  ,colorder :: Int  -- ^ ordering of column within the RTuple (each new column added takes colorder+1)
                                          --   Since an RTuple is implemented as a HashMap ColumnName RDataType, ordering of columns has no meaning.
                                          --   However, with this columns we can "pretend" that there is a fixed column order in each RTuple.
                        ,dtype :: ColumnDType
                    } deriving (Show, Eq)


-- | Creates a list of the form [(ColumnInfo, RDataType)]  from a list of ColumnInfo and an RTuple. The returned list respects the order of the [ColumnInfo]
    -- Prelude.zip listOfColInfo (Prelude.map (snd) $ HM.toList rtup)  -- this code does NOT guarantee that HM.toList will return the same column order as [ColumnInfo]
listOfColInfoRDataType :: [ColumnInfo] -> RTuple -> [(ColumnInfo, RDataType)]  -- this code does guarantees that RDataTypes will be in the same column order as [ColumnInfo], i.e., the correct RDataType for the correct column
listOfColInfoRDataType (ci:[]) rtup = [(ci, rtup HM.!(name ci))]  -- rt HM.!(name ci) -> this returns the RDataType by column name
listOfColInfoRDataType (ci:colInfos) rtup = (ci, rtup HM.!(name ci)):listOfColInfoRDataType colInfos rtup


-- | createRDataType:  Get a value of type a and return the corresponding RDataType.
-- The input value data type must be an instance of the Typepable typeclass from Data.Typeable
createRDataType ::
    TB.Typeable a
    => a            -- ^ input value
    -> RDataType    -- ^ output RDataType
createRDataType val = 
        case show (TB.typeOf val) of
                        --"Int"     -> RInt $ D.fromDyn (D.toDyn val) 0
                        "Int"     -> case (D.fromDynamic (D.toDyn val)) of   -- toDyn :: Typeable a => a -> Dynamic
                                                Just v -> RInt v             -- fromDynamic :: Typeable a    => Dynamic  -> Maybe a  
                                                Nothing -> Null
                        --"Char"    -> RChar $ D.fromDyn (D.toDyn val) 'a'
                     {--   "Char"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RChar v
                                                Nothing -> Null                        --}
                        --"Text"    -> RText $ D.fromDyn (D.toDyn val) ""
                        "Text"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RText v
                                                Nothing -> Null                                                
                        --"[Char]"  -> RString $ D.fromDyn (D.toDyn val) ""
                      {--  "[Char]"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RString v
                                                Nothing -> Null                        --}
                        --"Double"  -> RDouble $ D.fromDyn (D.toDyn val) 0.0
                        "Double"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RDouble v
                                                Nothing -> Null                                                
                        --"Float"   -> RFloat $ D.fromDyn (D.toDyn val) 0.0
                {--        "Float"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> RFloat v
                                                Nothing -> Null                       --}
                        _         -> Null


{--createRDataType ::
    TB.Typeable a
    => a            -- ^ input value
    -> RDataType    -- ^ output RDataType
createRDataType val = 
        case show (TB.typeOf val) of
                        --"Int"     -> RInt $ D.fromDyn (D.toDyn val) 0
                        "Int"     -> case (D.fromDynamic (D.toDyn val)) of   -- toDyn :: Typeable a => a -> Dynamic
                                                Just v ->  v             -- fromDynamic :: Typeable a    => Dynamic  -> Maybe a  
                                                Nothing -> Null
                        --"Char"    -> RChar $ D.fromDyn (D.toDyn val) 'a'
                        "Char"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                        
                        --"Text"    -> RText $ D.fromDyn (D.toDyn val) ""
                        "Text"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        --"[Char]"  -> RString $ D.fromDyn (D.toDyn val) ""
                        "[Char]"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        --"Double"  -> RDouble $ D.fromDyn (D.toDyn val) 0.0
                        "Double"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        --"Float"   -> RFloat $ D.fromDyn (D.toDyn val) 0.0
                        "Float"     -> case (D.fromDynamic (D.toDyn val)) of 
                                                Just v -> v
                                                Nothing -> Null                                                
                        _         -> Null
--}


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
      ROperationEmpty
    | RUnion   -- ^ Union 
    | RInter     -- ^ Intersection
    | RDiff    -- ^ Difference
    | RPrj    { colPrjList :: [ColumnName] }   -- ^ Projection
    | RFilter { fpred :: RPredicate }   -- ^ Filter
    | RInJoin { jpred :: RJoinPredicate }     -- ^ Inner Join (any type of join predicate allowed)
    | RLeftJoin { jpred :: RJoinPredicate }   -- ^ Left Outer Join (any type of join predicate allowed)    
    | RRightJoin { jpred :: RJoinPredicate }  -- ^ Right Outer Join (any type of join predicate allowed)        
    | RAggregate { aggList :: [RAggOperation] } -- Performs some aggregation operations on specific columns and returns a singleton RTable
    | RGroupBy  { gpred :: RGroupPredicate, aggList :: [RAggOperation], colGrByList :: [ColumnName] } -- ^ A GroupBy operation
                                                                                                      -- An SQL equivalent: SELECT colGrByList, aggList FROM... GROUP BY colGrByList
                                                                                                      -- Note that compared to SQL, we can have a more generic grouping predicate (i.e.,
                                                                                                      -- when two RTuples should belong in the same group) than just the equality of 
                                                                                                      -- values on the common columns between two RTuples.
                                                                                                      -- Also note, that in the case of an aggregation without grouping (equivalent to
                                                                                                      -- a single group group by), then the grouping predicate should be: 
                                                                                                      -- @
                                                                                                      -- \_ _ -> True
                                                                                                      -- @
    | RCombinedOp { rcombOp :: UnaryRTableOperation  }   -- ^ A combination of unary ROperations e.g.,   (p plist).(f pred)  (i.e., RPrj . RFilter), in the form of an RTable -> RTable function.
                                                     --  In this sense we can also include a binary operation (e.g. join), if we partially apply the join to one RTable
                                                     --  e.g., (ij jpred rtab) . (p plist) . (f pred)
    | RBinOp { rbinOp :: BinaryRTableOperation } -- ^ A generic binary ROperation.
    | ROrderBy { colOrdList :: [(ColumnName, OrderingSpec)] }   -- ^ Order the RTuples of the RTable acocrding to the specified list of Columns.
                                                                -- First column in the input list has the highest priority in the sorting order

-- | A sum type to help the specification of a column ordering (Ascending, or Descending)
data OrderingSpec = Asc | Desc deriving (Show, Eq)

-- | A generic unary operation on a RTable
type UnaryRTableOperation = RTable -> RTable

-- | A generic binary operation on RTable
type BinaryRTableOperation = RTable -> RTable -> RTable


 --deriving (Eq,Show)
    -- | RFieldRenaming
    -- | RAggregation
    -- | RExtension

-- | Definition of a Join Predicate
type RJoinPredicate = RTuple -> RTuple -> Bool

-- | Definition of a Group By Predicate
-- It defines the condition for two RTuples to be included in the same group.
type RGroupPredicate = RTuple -> RTuple -> Bool


-- | This data type represents all possible aggregate operations over an RTable.
-- Examples are : Sum, Count, Average, Min, Max but it can be any other "aggregation".
-- The essential property of an aggregate operation is that it acts on an RTable (or on 
-- a group of RTuples - in the case of the RGroupBy operation) and produces a single RTuple.
-- 
-- An aggregate operation is applied on a specific column (source column) and the aggregated result
-- will be stored in the target column. It is important to understand that the produced aggregated RTuple 
-- is different from the input RTuples. It is a totally new RTuple, that will consist of the 
-- aggregated column(s) (and the grouping columns in the case of an RGroupBy).

-- Also, note that following SQL semantics, an aggregate operation ignores  Null values.
-- So for example, a SUM(column) will just ignore them and also will COUNT(column), i.e., it 
-- will not sum or count the Nulls. If all columns are Null, then a Null will be returned.
--
data RAggOperation = RAggOperation {
                         sourceCol :: ColumnName    -- ^ Source column
                        ,targetCol :: ColumnName    -- ^ Target column
                        ,aggFunc :: RTable -> RTuple  -- ^ here we define the aggegate function to be applied on an RTable
                    }

-- | The following are all common aggregate operations

-- | The Sum aggregate operation
raggSum :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggSum src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg                                 
                ,aggFunc = \rtab -> createRtuple [(trg, sumFold src trg rtab)]  
        }

-- | A helper function in raggSum that implements the basic fold for sum aggregation        
sumFold :: ColumnName -> ColumnName -> RTable -> RDataType
sumFold src trg rtab =         
    V.foldr' ( \rtup accValue ->                                                                     
                    --if (getRTupColValue src) rtup /= Null && accValue /= Null
                    if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)
                        then
                            (getRTupColValue src) rtup + accValue
                        else
                            --if (getRTupColValue src) rtup == Null && accValue /= Null 
                            if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)
                                then
                                    accValue  -- ignore Null value
                                else
                                    --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                    if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                        then
                                            (getRTupColValue src) rtup + RInt 0  -- ignore so far Null agg result
                                                                                 -- add RInt 0, so in the case of a non-numeric rtup value, the result will be a Null
                                                                                 -- while if rtup value is numeric the addition of RInt 0 does not cause a problem
                                        else
                                            Null -- agg of Nulls is Null
             ) (Null) rtab


-- | The Count aggregate operation
raggCount :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggCount src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRtuple [(trg, countFold src trg rtab)]  
        }

-- | A helper function in raggCount that implements the basic fold for Count aggregation        
countFold :: ColumnName -> ColumnName -> RTable -> RDataType
countFold src trg rtab =         
    V.foldr' ( \rtup accValue ->                                                                     
                            --if (getRTupColValue src) rtup /= Null && accValue /= Null
                            if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)  
                                then
                                    RInt 1 + accValue
                                else
                                    --if (getRTupColValue src) rtup == Null && accValue /= Null 
                                    if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)  
                                        then
                                            accValue  -- ignore Null value
                                        else
                                            --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                            if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                                then
                                                    RInt 1  -- ignore so far Null agg result
                                                else
                                                    Null -- agg of Nulls is Null
             ) (Null) rtab


-- | The Average aggregate operation
raggAvg :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggAvg src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRtuple [(trg, let 
                                                             sum = sumFold src trg rtab
                                                             cnt =  countFold src trg rtab
                                                        in case (sum,cnt) of
                                                                (RInt s, RInt c) -> RDouble (fromIntegral s / fromIntegral c)
                                                                (RDouble s, RInt c) -> RDouble (s / fromIntegral c)
                                                                (_, _)           -> Null
                                                 )]  
        }        

-- | The Max aggregate operation
raggMax :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggMax src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRtuple [(trg, maxFold src trg rtab)]  
        }        


-- | A helper function in raggMax that implements the basic fold for Max aggregation        
maxFold :: ColumnName -> ColumnName -> RTable -> RDataType
maxFold src trg rtab =         
    V.foldr' ( \rtup accValue ->         
                                --if (getRTupColValue src) rtup /= Null && accValue /= Null
                                if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)  
                                    then
                                        max ((getRTupColValue src) rtup) accValue
                                    else
                                        --if (getRTupColValue src) rtup == Null && accValue /= Null 
                                        if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)  
                                            then
                                                accValue  -- ignore Null value
                                            else
                                                --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                                if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                                    then
                                                        (getRTupColValue src) rtup  -- ignore so far Null agg result
                                                    else
                                                        Null -- agg of Nulls is Null
             ) Null rtab


-- | The Min aggregate operation
raggMin :: 
        ColumnName -- ^ source column
    ->  ColumnName -- ^ target column
    ->  RAggOperation
raggMin src trg = RAggOperation {
                 sourceCol = src
                ,targetCol = trg
                ,aggFunc = \rtab -> createRtuple [(trg, minFold src trg rtab)]  
        }        

-- | A helper function in raggMin that implements the basic fold for Min aggregation        
minFold :: ColumnName -> ColumnName -> RTable -> RDataType
minFold src trg rtab =         
    V.foldr' ( \rtup accValue ->         
                                --if (getRTupColValue src) rtup /= Null && accValue /= Null
                                if (isNotNull $ (getRTupColValue src) rtup)  && (isNotNull accValue)
                                    then
                                        min ((getRTupColValue src) rtup) accValue
                                    else
                                        --if (getRTupColValue src) rtup == Null && accValue /= Null 
                                        if (isNull $ (getRTupColValue src) rtup) && (isNotNull accValue)  
                                            then
                                                accValue  -- ignore Null value
                                            else
                                                --if (getRTupColValue src) rtup /= Null && accValue == Null 
                                                if (isNotNull $ (getRTupColValue src) rtup) && (isNull accValue)
                                                    then
                                                        (getRTupColValue src) rtup  -- ignore so far Null agg result
                                                    else
                                                        Null -- agg of Nulls is Null
             ) Null rtab

{--
data RAggOperation = 
          RSum ColumnName  -- ^  sums values in the specific column
        | RCount ColumnName -- ^ count of values in the specific column
        | RCountDist ColumnName -- ^ distinct count of values in the specific column
        | RAvg ColumnName  -- ^ average of values in the specific column
        | RMin ColumnName -- ^ minimum of values in the specific column
        | RMax ColumnName -- ^ maximum of values in the specific column
--}

-- | ropU operator executes a unary ROperation
ropU = runUnaryROperation

-- | Execute a Unary ROperation
runUnaryROperation :: 
    ROperation -- ^ input ROperation
    -> RTable  -- ^ input RTable
    -> RTable  -- ^ output RTable
runUnaryROperation rop irtab = 
    case rop of
        RFilter { fpred = rpredicate }                                                  ->  runRfilter rpredicate irtab
        RPrj { colPrjList = colNames }                                                  ->  runProjection colNames irtab
        RAggregate { aggList = aggFunctions }                                           ->  runAggregation aggFunctions irtab
        RGroupBy  { gpred = groupingpred, aggList = aggFunctions, colGrByList = cols }  ->  runGroupBy groupingpred aggFunctions cols irtab
        RCombinedOp { rcombOp = comb }                                                  ->  runCombinedROp comb irtab 
        ROrderBy { colOrdList = colist }                                                ->  runOrderBy colist irtab


-- | ropB operator executes a binary ROperation
ropB = runBinaryROperation

-- | Execute a Binary ROperation
runBinaryROperation :: 
    ROperation -- ^ input ROperation
    -> RTable  -- ^ input RTable1
    -> RTable  -- ^ input RTable2    
    -> RTable  -- ^ output RTabl
runBinaryROperation rop irtab1 irtab2 = 
    case rop of
        RInJoin    { jpred = jpredicate } -> runInnerJoin jpredicate irtab1 irtab2
        RLeftJoin  { jpred = jpredicate } -> runLeftJoin jpredicate irtab1 irtab2
        RRightJoin { jpred = jpredicate } -> runRightJoin jpredicate irtab1 irtab2
        RUnion -> runUnion irtab1 irtab2
        RInter -> runIntersect irtab1 irtab2
        RDiff  -> runDiff irtab1 irtab2  
        RBinOp { rbinOp = bop } -> bop irtab1 irtab2      


-- * #########  Construction ##########

-- | Test whether an RTable is empty
isRTabEmpty :: RTable -> Bool
isRTabEmpty = V.null

-- | Test whether an RTuple is empty
isRTupEmpty :: RTuple -> Bool
isRTupEmpty = HM.null

-- | emptyRTable: Create an empty RTable
emptyRTable :: RTable
emptyRTable = V.empty :: RTable

-- | Creates an empty RTuple (i.e., one with no column,value mappings)
emptyRTuple :: RTuple 
emptyRTuple = HM.empty


-- | Creates an RTable with a single RTuple
createSingletonRTable ::
       RTuple 
    -> RTable 
createSingletonRTable rt = V.singleton rt

-- | createRTuple: Create an Rtuple from a list of column names and values
createRtuple ::
      [(ColumnName, RDataType)]  -- ^ input list of (columnname,value) pairs
    -> RTuple 
createRtuple l = HM.fromList l



-- | Creates a Null RTuple based on a list of input Column Names.
-- A Null RTuple is an RTuple where all column names correspond to a Null value (Null is a data constructor of RDataType)
createNullRTuple ::
       [ColumnName]
    -> RTuple
createNullRTuple cnames = HM.fromList $ zipped
    where zipped = Data.List.zip cnames (Data.List.take (Data.List.length cnames) (repeat Null))

-- | Returns True if the input RTuple is a Null RTuple, otherwise it returns False
isNullRTuple ::
       RTuple 
    -> Bool    
isNullRTuple t = 
    let -- if t is really Null, then the following must return an empty RTuple (since a Null RTuple has all its values equal with Null)
        checkt = HM.filter (\v -> isNotNull  v) t  --v /= Null) t 
    in if isRTupEmpty checkt 
            then True
            else False


-- * ########## RTable "Functional" Operations ##############

-- | This is a fold operation on a RTable.
-- It is similar with :
-- @
--  foldr' :: (a -> b -> b) -> b -> Vector a -> b Source #
-- @
-- of Vector, which is an O(n) Right fold with a strict accumulator
rtabFoldr' :: (RTuple -> RTable -> RTable) -> RTable -> RTable -> RTable
rtabFoldr' f accum rtab = V.foldr' f accum rtab

-- | This is a fold operation on a RTable.
-- It is similar with :
-- @
--  foldl' :: (a -> b -> a) -> a -> Vector b -> a
-- @
-- of Vector, which is an O(n) Left fold with a strict accumulator
rtabFoldl' :: (RTable -> RTuple -> RTable) -> RTable -> RTable -> RTable
rtabFoldl' f accum rtab = V.foldl' f accum rtab


-- | Map function over an RTable
rtabMap :: (RTuple -> RTuple) -> RTable -> RTable
rtabMap f rtab = V.map f rtab 

-- * ########## RTable Relational Operations ##############

-- | Number of RTuples returned by an RTable operation
type RTuplesRet = Sum Int

-- | Creates an RTuplesRet type
rtuplesRet :: Int -> RTuplesRet
rtuplesRet i = (M.Sum i) :: RTuplesRet

-- | Return the number embedded in the RTuplesRet data type
getRTuplesRet :: RTuplesRet -> Int 
getRTuplesRet = M.getSum


-- | RTabResult is the result of an RTable operation and is a Writer Monad, that includes the new RTable, 
-- as well as the number of RTuples returned by the operation.
type RTabResult = Writer RTuplesRet RTable

-- | Creates an RTabResult (i.e., a Writer Monad) from a result RTable and the number of RTuples that it returned
rtabResult :: 
       (RTable, RTuplesRet)  -- ^ input pair 
    -> RTabResult -- ^ output Writer Monad
rtabResult (rtab, rtupRet) = writer (rtab, rtupRet)

-- | Returns the info "stored" in the RTabResult Writer Monad
runRTabResult ::
       RTabResult
    -> (RTable, RTuplesRet)
runRTabResult rtr = runWriter rtr

-- | Returns the "log message" in the RTabResult Writer Monad, which is the number of returned RTuples
execRTabResult ::
       RTabResult
    -> RTuplesRet
execRTabResult rtr = execWriter rtr  


-- | removeColumn : removes a column from an RTable.
--   The column is specified by ColumnName.
--   If this ColumnName does not exist in the RTuple of the input RTable
--   then nothing is happened, the RTuple remains intact.
removeColumn ::
       ColumnName  -- ^ Column to be removed
    -> RTable      -- ^ input RTable 
    -> RTable      -- ^ output RTable 
removeColumn col rtabSrc = do
      srcRtup <- rtabSrc
      let targetRtup = HM.delete col srcRtup  
      return targetRtup

-- | addColumn: adds a column to an RTable
addColumn ::
      ColumnName      -- ^ name of the column to be added
  ->  RDataType       -- ^ Default value of the new column. All RTuples will initially have this value in this column
  ->  RTable          -- ^ Input RTable
  ->  RTable          -- ^ Output RTable
addColumn name initVal rtabSrc = do
    srcRtup <- rtabSrc
    let targetRtup = HM.insert name initVal srcRtup
    return targetRtup


-- | RTable Filter operator
f = runRfilter

-- | Executes an RFilter operation
runRfilter ::
    RPredicate
    -> RTable
    -> RTable
runRfilter rpred rtab = 
    if isRTabEmpty rtab
        then emptyRTable
        else 
            V.filter rpred rtab

-- | RTable Projection operator
p = runProjection

-- | Implements RTable projection operation.
-- If a column name does not exist, then an empty RTable is returned.
runProjection :: 
    [ColumnName]  -- ^ list of column names to be included in the final result RTable
    -> RTable
    -> RTable
runProjection colNamList irtab = 
    if isRTabEmpty irtab
        then
            emptyRTable
        else
            do -- RTable is a Monad
                srcRtuple <- irtab
                let
                    -- 1. get original column value (in this case it is a list of values :: Maybe RDataType)
                    srcValueL = Data.List.map (\colName -> rtupLookup colName srcRtuple) colNamList

                -- if there is at least one Nothing value then an non-existing column name has been asked. 
                if Data.List.elem Nothing srcValueL 
                    then -- return an empty RTable
                        emptyRTable
                    else
                        let 
                            -- 2. create the new RTuple                        
                            valList = Data.List.map (\(Just v) -> v) srcValueL -- get rid of Maybe
                            targetRtuple = rtupleFromList (Data.List.zip colNamList valList) -- HM.fromList
                        in return targetRtuple                        


-- | Implements RTable projection operation.
-- If a column name does not exist, then the returned RTable includes this column with a Null
-- value. This projection implementation allows missed hits.
runProjectionMissedHits :: 
    [ColumnName]  -- ^ list of column names to be included in the final result RTable
    -> RTable
    -> RTable
runProjectionMissedHits colNamList irtab = 
    if isRTabEmpty irtab
        then
            emptyRTable
        else
            do -- RTable is a Monad
                srcRtuple <- irtab
                let
                    -- 1. get original column value (in this case it is a list of values)
                    srcValueL = Data.List.map (\src -> HM.lookupDefault       Null -- return Null if value cannot be found based on column name 
                                                                    src   -- column name to look for (source) - i.e., the key in the HashMap
                                                                    srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                                    ) colNamList
                    -- 2. create the new RTuple
                    targetRtuple = HM.fromList (Data.List.zip colNamList srcValueL)
                return targetRtuple


-- | restrictNrows: returns the N first rows of an RTable
restrictNrows ::
       Int          -- ^ number of N rows to select
    -> RTable       -- ^ input RTable
    -> RTable       -- ^ output RTable
restrictNrows n r1 = V.take n r1

-- | RTable Inner Join Operator
iJ = runInnerJoinO

-- | Implements an Inner Join operation between two RTables (any type of join predicate is allowed)
-- Note that this operation is implemented as a 'Data.HashMap.Strict' union, which means "the first 
-- Map (i.e., the left RTuple) will be prefered when dublicate keys encountered with different values. That is, in the context of 
-- joining two RTuples the value of the first (i.e., left) RTuple on the common key will be prefered.
runInnerJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runInnerJoin jpred irtab1 irtab2 =  
    if (isRTabEmpty irtab1) || (isRTabEmpty irtab2)
        then
            emptyRTable
        else 
            do 
                rtup1 <- irtab1
                rtup2 <- irtab2
                let targetRtuple = 
                        if (jpred rtup1 rtup2)
                        then HM.union rtup1 rtup2                 
                        else HM.empty
                removeEmptyRTuples (return targetRtuple)
                    where removeEmptyRTuples = f (not.isRTupEmpty) 

-- Inner Join with Oracle DB's convention for common column names.
-- When we have two tuples t1 and t2 with a common column name (lets say "Common"), then the resulting tuple after a join
-- will be "Common", "Common_1", so a "_1" suffix is appended. The tuple from the left table by convention retains the original column name.
-- So "Column_1" is the column from the right table. If "Column_1" already exists, then "Column_2" is used.
runInnerJoinO ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runInnerJoinO jpred tabDriver tabProbed =  
    if isRTabEmpty tabDriver || isRTabEmpty tabProbed
        then
            emptyRTable
        else 
            do 
                rtupDrv <- tabDriver
                -- this is the equivalent of a nested loop with tabDriver playing the role of the driving table and tabProbed the probed table
                V.foldr' (\t accum -> 
                            if (jpred rtupDrv t) 
                                then 
                                    -- insert joined tuple to result table (i.e. the accumulator)
                                    insertAppendRTab (joinRTuples rtupDrv t) accum
                                else 
                                    -- keep the accumulator unchanged
                                    accum
                        ) emptyRTable tabProbed 


-- | Joins two RTuples into one. 
-- In this join we follow Oracle DB's convention when joining two tuples with some common column names.
-- When we have two tuples t1 and t2 with a common column name (lets say "Common"), then the resulitng tuple after a join
-- will be "Common", "Common_1", so a "_1" suffix is appended. The tuple from the left table by convention retains the original column name.
-- So "Column_1" is the column from the right table.
-- If "Column_1" already exists, then "Column_2" is used.
joinRTuples :: RTuple -> RTuple -> RTuple
joinRTuples tleft tright = 
    let
        -- change keys in tright what needs to be renamed because also appear in tleft
        -- first keep a copy of the tright pairs that dont need a key change
        dontNeedChange  = HM.difference tright tleft
        changedPart = changeKeys tleft tright
        -- create  a new version of tright, with no common keys with tleft
        new_tright = HM.union dontNeedChange changedPart
    in HM.union tleft new_tright  
        where 
            -- rename keys of right rtuple until there no more common keys with the left rtuple            
            changeKeys :: RTuple -> RTuple -> RTuple
            changeKeys tleft changedPart = 
                if isRTupEmpty (HM.intersection changedPart tleft)
                    then -- we are done, no more common keys
                        changedPart
                    else
                        -- there are still common keys to change
                        let
                            needChange = HM.intersection changedPart tleft -- (k,v) pairs that exist in changedPart and the keys also appear in tleft. Thus these keys have to be renamed
                            dontNeedChange  = HM.difference changedPart tleft -- (k,v) pairs that exist in changedPart and the keys dont appear in tleft. Thus these keys DONT have to be renamed
                            new_changedPart =  fromList $ Data.List.map (\(k,v) -> (newKey k, v)) $ toList needChange
                        in HM.union dontNeedChange (changeKeys tleft new_changedPart)
                          
            -- generate a new key as this:
            -- "hello" -> "hello_1"
            -- "hello_1" -> "hello_2"
            -- "hello_2" -> "hello_3"
            newKey :: ColumnName -> ColumnName
            newKey name  = 
                let 
                    lastChar = Data.List.last name
                    beforeLastChar = name !! (Data.List.length name - 2)
                in  
                    if beforeLastChar == '_'  &&  Data.Char.isDigit lastChar
                                    then (Data.List.take (Data.List.length name - 2) name) ++ ( '_' : (show $ (read (lastChar : "") :: Int) + 1) )
                                    else name ++ "_1"


-- | RTable Left Outer Join Operator
lJ = runLeftJoin

-- | Implements a Left Outer Join operation between two RTables (any type of join predicate is allowed),
-- i.e., the rows of the left RTable will be preserved.
-- Note that when dublicate keys encountered that is, since the underlying structure for an RTuple is a Data.HashMap.Strict,
-- only one value per key is allowed. So in the context of joining two RTuples the value of the left RTuple on the common key will be prefered.
{-runLeftJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runLeftJoin jpred leftRTab rtab = do
    rtupLeft <- leftRTab
    rtup <- rtab
    let targetRtuple = 
            if (jpred rtupLeft rtup)
            then HM.union rtupLeft rtup
            else HM.union rtupLeft (createNullRTuple colNamesList)
                    where colNamesList = HM.keys rtup
    --return targetRtuple

    -- remove repeated rtuples and keep just the rtuples of the preserving rtable
    iJ (jpred2) (return targetRtuple) leftRTab
        where 
            -- the left tuple is the extended (result of the outer join)
            -- the right tuple is from the preserving table
            -- we need to return True for those who are equal in the common set of columns
            jpred2 tL tR = 
                let left = HM.toList tL
                    right = HM.toList tR
                    -- in order to satisfy the join pred, all elements of right must exist in left
                in  Data.List.all (\(k,v) -> Data.List.elem (k,v) left) right
-}

-- | Implements a Left Outer Join operation between two RTables (any type of join predicate is allowed),
-- i.e., the rows of the left RTable will be preserved.
-- A Left Join :
-- @ 
-- tabLeft LEFT JOIN tabRight ON joinPred
-- @ 
-- where tabLeft is the preserving table can be defined as:
-- the Union between the following two RTables:
--  A. The result of the inner join: tabLeft INNER JOIN tabRight ON joinPred
--  B. The rows from the preserving table (tabLeft) that DONT satisfy the join condition, enhanced with the columns
--     of tabRight returning Null values.
--  The common columns will appear from both tables but only the left table column's will retain their original name. 
runLeftJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runLeftJoin jpred preservingTab tab = 
    if isRTabEmpty preservingTab
        then 
            emptyRTable
        else
            if isRTabEmpty tab
                then
                    -- return the preserved tab 
                    preservingTab

                else 
                    -- we know that both the preservingTab and tab are non empty 
                    let 
                        unionFstPart = iJ jpred preservingTab tab 
                        -- project only the preserving tab's columns
                        fstPartProj = p (getColumnNamesfromRTab preservingTab) unionFstPart

                        -- the second part are the rows from the preserving table that dont join
                        -- we will use the Difference operations for this
                        unionSndPart = 
                            let 
                                difftab = d preservingTab fstPartProj -- unionFstPart
                                -- now enhance the result with the columns of the right table
                            in iJ (\t1 t2 -> True) difftab (createSingletonRTable $ createNullRTuple $ (getColumnNamesfromRTab tab))
                    in u unionFstPart unionSndPart


-- | RTable Right Outer Join Operator
rJ = runRightJoin

-- | Implements a Right Outer Join operation between two RTables (any type of join predicate is allowed),
-- i.e., the rows of the right RTable will be preserved.
-- A Right Join :
-- @ 
-- tabLeft RIGHT JOIN tabRight ON joinPred
-- @ 
-- where tabRight is the preserving table can be defined as:
-- the Union between the following two RTables:
--  A. The result of the inner join: tabLeft INNER JOIN tabRight ON joinPred
--  B. The rows from the preserving table (tabRight) that DONT satisfy the join condition, enhanced with the columns
--     of tabLeft returning Null values.
--  The common columns will appear from both tables but only the right table column's will retain their original name. 
runRightJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runRightJoin jpred tab preservingTab =
    if isRTabEmpty preservingTab
        then 
            emptyRTable
        else
            if isRTabEmpty tab
                then
                    preservingTab        
                else     
                    -- we know that both the preservingTab and tab are non empty 
                    let 
                        unionFstPart =                             
                            iJ (flip jpred) preservingTab tab --tab    -- we used the preserving table as the left table in the inner join, 
                                                                    -- in order to retain the original column names for the common columns 
                        -- debug
                        -- !dummy1 = trace ("unionFstPart:\n" ++ show unionFstPart) True

                        -- project only the preserving tab's columns
                        fstPartProj = 
                            p (getColumnNamesfromRTab preservingTab) unionFstPart

                        -- the second part are the rows from the preserving table that dont join
                        -- we will use the Difference operations for this
                        unionSndPart = 
                            let 
                                difftab = 
                                    d preservingTab   fstPartProj -- unionFstPart 
                                -- now enhance the result with the columns of the left table
                            in iJ (\t1 t2 -> True) difftab (createSingletonRTable $ createNullRTuple $ (getColumnNamesfromRTab tab))
                        -- debug
                        -- !dummy2 = trace ("unionSndPart :\n" ++ show unionSndPart) True
                        -- !dummy3 = trace ("u unionFstPart unionSndPart :\n" ++ (show $ u unionFstPart unionSndPart)) True
                    in u unionFstPart unionSndPart



-- | Implements a Right Outer Join operation between two RTables (any type of join predicate is allowed)
-- i.e., the rows of the right RTable will be preserved.
-- Note that when dublicate keys encountered that is, since the underlying structure for an RTuple is a Data.HashMap.Strict,
-- only one value per key is allowed. So in the context of joining two RTuples the value of the right RTuple on the common key will be prefered.
{-runRightJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runRightJoin jpred rtab rightRTab = do
    rtupRight <- rightRTab
    rtup <- rtab
    let targetRtuple = 
            if (jpred rtup rtupRight)
            then HM.union rtupRight rtup
            else HM.union rtupRight (createNullRTuple colNamesList)
                    where colNamesList = HM.keys rtup
    return targetRtuple-}

-- | RTable Full Outer Join Operator
foJ = runFullOuterJoin

-- | Implements a Full Outer Join operation between two RTables (any type of join predicate is allowed)
-- A full outer join is the union of the left and right outer joins respectively.
-- The common columns will appear from both tables but only the left table column's will retain their original name (just by convention).
runFullOuterJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runFullOuterJoin jpred leftRTab rightRTab = -- (lJ jpred leftRTab rightRTab) `u` (rJ jpred leftRTab rightRTab) -- (note that `u` eliminates dublicates)
    let
        --
        -- we want to get to this union:
        -- (lJ jpred leftRTab rightRTab) `u` (d (rJ jpred leftRTab rightRTab) (ij (flip jpred) rightRTab leftRTab))
        -- 
        -- The problem is with the change of column names that are common in both tables. In the right part of the union
        -- rightTab has preserved its original column names while in the left part the have changed to "_1" suffix. 
        -- So we cant do the union as is, we need to change the second part of the union (right one) to have the same column names
        -- as the firts part, i.e., original names for leftTab and changed names for rightTab.
        --
        unionFstPart = lJ jpred leftRTab rightRTab -- in unionFstPart rightTab's columns have changed names
        
        -- we need to construct the 2nd part of the union with leftTab columns unchanged and rightTab changed        
        unionSndPartTemp1 = d (rJ jpred leftRTab rightRTab) (iJ (flip jpred) rightRTab leftRTab)        
        -- isolate the columns of the rightTab
        unionSndPartTemp2 = p (getColumnNamesfromRTab rightRTab) unionSndPartTemp1
        -- this join is a trick in order to change names of the rightTab
        unionSndPart = iJ (\t1 t2 -> True) (createSingletonRTable $ createNullRTuple $ (getColumnNamesfromRTab leftRTab)) unionSndPartTemp2
    in unionFstPart `u` unionSndPart

-- | RTable Union Operator
u = runUnion

-- We cannot implement Union like the following (i.e., union of lists) because when two identical RTuples that contain Null values are checked for equality
-- equality comparison between Null returns always false. So we have to implement our own union using the isNull function.

-- | Implements the union of two RTables as a union of two lists (see 'Data.List').
-- Duplicates, and elements of the first list, are removed from the the second list, but if the first list contains duplicates, so will the result
{-runUnion :: 
    RTable
    -> RTable
    -> RTable
runUnion rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = Data.List.union ls1 ls2
    in  V.fromList resultLs
-}

-- | Implements the union of two RTables as a union of two lists (see 'Data.List').
runUnion :: 
    RTable
    -> RTable
    -> RTable
runUnion rt1 rt2 =
    if isRTabEmpty rt1 && isRTabEmpty rt2
        then 
            emptyRTable
        else
            if isRTabEmpty rt1
                then rt2 
                else
                    if isRTabEmpty rt2
                        then rt1
                        else  
                            -- construct the union result by concatenating the left table with the subset of tuple from the right table that do
                            -- not appear in the left table (i.e, remove dublicates)
                            rt1 V.++ (V.foldr (un) emptyRTable rt2)
                            where
                                un :: RTuple -> RTable -> RTable
                                un tupRight acc = 
                                    -- Can we find tupRight in the left table?            
                                    if didYouFindIt tupRight rt1 
                                        then acc   -- then discard tuplRight ,leave result unchanged          
                                        else V.snoc acc tupRight  -- else insert tupRight into final result



-- | RTable Intersection Operator
i = runIntersect

-- | Implements the intersection of two RTables 
runIntersect :: 
    RTable
    -> RTable
    -> RTable
runIntersect rt1 rt2 =
    if isRTabEmpty rt1 || isRTabEmpty rt2 
        then
            emptyRTable
        else 
            -- construct the intersect result by traversing the left table and checking if each tuple exists in the right table
            V.foldr (intsect) emptyRTable rt1
            where
                intsect :: RTuple -> RTable -> RTable
                intsect tupLeft acc = 
                    -- Can we find tupLeft in the right table?            
                    if didYouFindIt tupLeft rt2 
                        then V.snoc acc tupLeft  -- then insert tupLeft into final result
                        else acc  -- else discard tuplLeft ,leave result unchanged          

{-
runIntersect rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = Data.List.intersect ls1 ls2
    in  V.fromList resultLs

-}

-- | RTable Difference Operator
d = runDiff

-- Test it with this, from ghci:
-- ---------------------------------
-- :set -XOverloadedStrings
-- :m + Data.HashMap.Strict
--
-- let t1 = fromList [("c1",RInt 1),("c2", Null)]
-- let t2 = fromList [("c1",RInt 2),("c2", Null)]
-- let t3 = fromList [("c1",RInt 3),("c2", Null)]
-- let t4 = fromList [("c1",RInt 4),("c2", Null)]
-- :m + Data.Vector
-- let tab1 = Data.Vector.fromList [t1,t2,t3,t4]
-- let tab2 = Data.Vector.fromList [t1,t2]

-- > d tab1 tab2
-- ---------------------------------

-- | Implements the set Difference of two RTables as the diff of two lists (see 'Data.List').
runDiff :: 
    RTable
    -> RTable
    -> RTable
runDiff rt1 rt2 =
    if isRTabEmpty rt1
        then
            emptyRTable
        else 
            if isRTabEmpty rt2
                then
                    rt1
                else  
                    -- construct the diff result by traversing the left table and checking if each tuple exists in the right table
                    V.foldr (diff) emptyRTable rt1
                    where
                        diff :: RTuple -> RTable -> RTable
                        diff tupLeft acc = 
                            -- Can we find tupLeft in the right table?            
                            if didYouFindIt tupLeft rt2 
                                then acc  -- then discard tuplLeft ,leave result unchanged
                                else V.snoc acc tupLeft  -- else insert tupLeft into final result

-- Important Note:
-- we need to implement are own equality comparison function "areTheyEqual" and not rely on the instance of Eq defined for RDataType above
-- because of "Null Logic". If we compare two RTuples that have Null values in any column, then these can never be equal, because
-- Null == Null returns False.
-- However, in SQL when you run a minus or interesection between two tables containing Nulls, it works! For example:
-- with q1
-- as (
--     select rownum c1, Null c2
--     from dual
--     connect by level < 5
-- ),
-- q2
-- as (
--     select rownum c1, Null c2
--     from dual
--     connect by level < 3
-- )
-- select *
-- from q1
-- minus
-- select *
-- from q2
-- 
-- q1:
-- C1  | C2 
-- ---   ---
-- 1   |                                      
-- 2   |                                     
-- 3   |                                      
-- 4   |                                     
--
-- q2:
-- C1  | C2 
-- ---   ---
-- 1   |                                      
-- 2   |                                     
--
-- And it will return:                
-- C1  | C2 
-- ---   ---
-- 3   |                                      
-- 4   |                                     
-- So for Minus and Intersection, when we compare RTuples we need to "bypass" the Null Logic
didYouFindIt :: RTuple -> RTable -> Bool
didYouFindIt searchTup tab = 
    V.foldr (\t acc -> (areTheyEqual searchTup t) || acc) False tab 
areTheyEqual :: RTuple -> RTuple -> Bool
areTheyEqual t1 t2 =  -- foldrWithKey :: (k -> v -> a -> a) -> a -> HashMap k v -> a
    HM.foldrWithKey (accumulator) True t1
        where 
            accumulator :: ColumnName -> RDataType -> Bool -> Bool
            accumulator colName val acc = 
                case val of
                    Null -> 
                            case (t2<!>colName) of
                                Null -> acc -- -- i.e., True && acc ,if both columns are Null then return equality
                                _    -> False -- i.e., False && acc
                    _    -> case (t2<!>colName) of
                                Null -> False -- i.e., False && acc
                                _    -> (val == t2<!>colName) && acc -- else compare them as normal

                        -- *** NOTE ***
                        -- the following piece of code does not work because val == Null always returns False !!!!

                        -- if (val == Null) && (t2<!>colName == Null) 
                        --     then acc -- i.e., True && acc
                        --     -- else compare them as normal
                        --     else  (val == t2<!>colName) && acc

{-
runDiff rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = ls1 Data.List.\\ ls2
    in  V.fromList resultLs
-}
            
rAgg = runAggregation

-- | Implements the aggregation operation on an RTable
-- It aggregates the specific columns in each AggOperation and returns a singleton RTable 
-- i.e., an RTable with a single RTuple that includes only the agg columns and their aggregated value.
runAggregation ::
        [RAggOperation]  -- ^ Input Aggregate Operations
    ->  RTable          -- ^ Input RTable
    ->  RTable          -- ^ Output singleton RTable
runAggregation [] rtab = rtab
runAggregation aggOps rtab =          
    if isRTabEmpty rtab
        then emptyRTable
        else
            createSingletonRTable (getResultRTuple aggOps rtab)
            where
                -- creates the final aggregated RTuple by applying all agg operations in the list
                -- and UNIONs all the intermediate agg RTuples to a final aggregated Rtuple
                getResultRTuple :: [RAggOperation] -> RTable -> RTuple
                getResultRTuple [] _ = emptyRTuple
                getResultRTuple (agg:aggs) rt =                    
                    let RAggOperation { sourceCol = src, targetCol = trg, aggFunc = aggf } = agg                        
                    in (getResultRTuple aggs rt) `HM.union` (aggf rt)   -- (aggf rt) `HM.union` (getResultRTuple aggs rt) 

rO = runOrderBy

-- | Implements the ORDER BY operation.
-- First column in the input list has the highest priority in the sorting order
-- We treat Null as the maximum value (anything compared to Null is smaller).
-- This way Nulls are send at the end (i.e.,  "Nulls Last" in SQL parlance). This is for Asc ordering.
-- For Desc ordering, we have the opposite. Nulls go first and so anything compared to Null is greater.
-- @
--      SQL example
-- with q 
-- as (select case when level < 4 then level else NULL end c1 -- , level c2
-- from dual
-- connect by level < 7
-- ) 
-- select * 
-- from q
-- order by c1
--
-- C1
--    ----
--     1
--     2
--     3
--     Null
--     Null
--     Null
--
-- with q 
-- as (select case when level < 4 then level else NULL end c1 -- , level c2
-- from dual
-- connect by level < 7
-- ) 
-- select * 
-- from q
-- order by c1 desc

--     C1
--     --
--     Null
--     Null    
--     Null
--     3
--     2
--     1
-- @
runOrderBy ::
        [(ColumnName, OrderingSpec)]  -- ^ Input ordering specification
    ->  RTable -- ^ Input RTable
    ->  RTable -- ^ Output RTable
runOrderBy ordSpec rtab = 
    if isRTabEmpty rtab
        then emptyRTable
    else 
        let unsortedRTupList = rtableToList rtab
            sortedRTupList = Data.List.sortBy (\t1 t2 -> compareTuples ordSpec t1 t2) unsortedRTupList
        in rtableFromList sortedRTupList
        where 
            compareTuples :: [(ColumnName, OrderingSpec)] -> RTuple -> RTuple -> Ordering
            compareTuples [] t1 t2 = EQ
            compareTuples ((col, colordspec) : rest) t1 t2 = 
                -- if they are equal or both Null on the column in question, then go to the next column
                if nvl (t1 <!> col) (RText "I am Null baby!") == nvl (t2 <!> col) (RText "I am Null baby!")
                    then compareTuples rest t1 t2
                    else -- Either one of the two is Null or are Not Equal
                         -- so we need to compare t1 versus t2
                         -- the GT, LT below refer to t1 wrt to t2
                         -- In the following we treat Null as the maximum value (anything compared to Null is smaller).
                         -- This way Nulls are send at the end (i.e., the default is "Nulls Last" in SQL parlance)
                        if isNull (t1 <!> col)
                            then 
                                case colordspec of
                                    Asc ->  GT -- t1 is GT than t2 (Nulls go to the end)
                                    Desc -> LT
                            else 
                                if isNull (t2 <!> col)
                                    then 
                                        case colordspec of
                                            Asc ->  LT -- t1 is LT than t2 (Nulls go to the end)
                                            Desc -> GT
                                else
                                    -- here we cant have Nulls
                                    case compare (t1 <!> col) (t2 <!> col) of
                                        GT -> if colordspec == Asc 
                                                then GT else LT
                                        LT -> if colordspec == Asc 
                                                then LT else GT

rG = runGroupBy

-- RGroupBy  { gpred :: RGroupPredicate, aggList :: [RAggOperation], colGrByList :: [ColumnName] }

-- hint: use Data.List.GroupBy for the grouping and Data.List.foldl' for the aggregation in each group
{--type RGroupPredicate = RTuple -> RTuple -> Bool

data RAggOperation = 
          RSum ColumnName  -- ^  sums values in the specific column
        | RCount ColumnName -- ^ count of values in the specific column
        | RCountDist ColumnName -- ^ distinct count of values in the specific column
        | RAvg ColumnName  -- ^ average of values in the specific column
        | RMin ColumnName -- ^ minimum of values in the specific column
        | RMax ColumnName
--}

-- | Implements the GROUP BY operation over an RTable.
runGroupBy ::
       RGroupPredicate   -- ^ Grouping predicate, in order to form the groups of RTuples (it defines when two RTuples should be included in the same group)
    -> [RAggOperation]   -- ^ Aggregations to be applied on specific columns
    -> [ColumnName]      -- ^ List of grouping column names (GROUP BY clause in SQL)
                         --   We assume that all RTuples in the same group have the same value in these columns
    -> RTable            -- ^ input RTable
    -> RTable            -- ^ output RTable
runGroupBy gpred aggOps cols rtab =  
    if isRTabEmpty rtab
        then emptyRTable
        else 
            let -- rtupList = V.toList rtab
                
                -- 1. form the groups of RTuples
                    -- a. first sort the Rtuples based on the grouping columns
                    -- This is a very important step if we want the groupBy operation to work. This is because grouping on lists is
                    -- implemented like this: group "Mississippi" = ["M","i","ss","i","ss","i","pp","i"]
                    -- So we have to sort the list first in order to get the right grouping:
                    -- group (sort "Mississippi") = ["M","iiii","pp","ssss"]
                
                listOfRTupSorted = rtableToList $ runOrderBy (createOrderingSpec cols) rtab

                -- debug
               -- !dummy1 = trace (show listOfRTupSorted) True

                   -- Data.List.sortBy (\t1 t2 -> if (gpred t1 t2) then EQ else compare (rtupleToList t1) (rtupleToList t2) ) rtupList --(t1!(Data.List.head . HM.keys $ t1)) (t2!(Data.List.head . HM.keys $ t2)) ) rtupList
                                   
                                   -- Data.List.map (HM.fromList) $ Data.List.sort $ Data.List.map (HM.toList) rtupList 
                                    --(t1!(Data.List.head . HM.keys $ t1)) (t2!(Data.List.head . HM.keys $ t2)) ) rtupList
                    
                    -- b then produce the groups
                listOfRTupGroupLists = Data.List.groupBy gpred listOfRTupSorted       

                -- debug
              --  !dummy2 = trace (show listOfRTupGroupLists) True

                -- 2. turn each (sub)list of Rtuples representing a Group into an RTable in order to apply aggregation
                --    Note: each RTable in this list will hold a group of RTuples that all have the same values in the input grouping columns
                --    (which must be compatible with the input grouping predicate)
                listofGroupRtabs = Data.List.map (rtableFromList) listOfRTupGroupLists
                -- 3. We need to keep the values of the grouping columns (e.g., by selecting the fisrt row) from each one of these RTables,
                --    in order to "join them" with the aggregated RTuples that will be produced by the aggregation operations
                --    The following will produce a list of singleton RTables.
                listOfGroupingColumnsRtabs = Data.List.map ( (restrictNrows 1) . (p cols) ) listofGroupRtabs

                -- debug
              --  !dummy3 = trace (show listOfGroupingColumnsRtabs) True

                -- 4. Aggregate each group according to input and produce a list of (aggregated singleton RTables)
                listOfAggregatedRtabs = Data.List.map (rAgg aggOps) listofGroupRtabs

                -- debug
              --  !dummy4 = trace (show listOfAggregatedRtabs ) True

                -- 5. Join the two list of singleton RTables
                listOfFinalRtabs = Data.List.zipWith (iJ (\t1 t2 -> True)) listOfGroupingColumnsRtabs listOfAggregatedRtabs

                -- debug
              --  !dummy5 = trace (show listOfFinalRtabs) True


                -- 6. Union all individual singleton RTables into the final RTable
            in  Data.List.foldr1 (u) listOfFinalRtabs
            where
                createOrderingSpec :: [ColumnName] -> [(ColumnName, OrderingSpec)]
                createOrderingSpec cols = Data.List.zip cols (Data.List.take (Data.List.length cols) $ Data.List.repeat Asc)


rComb = runCombinedROp

-- | runCombinedROp: A Higher Order function that accepts as input a combination of unary ROperations e.g.,   (p plist).(f pred)
--   expressed in the form of a function (RTable -> Rtable) and applies this function to the input RTable.
--  In this sense we can also include a binary operation (e.g. join), if we partially apply the join to one RTable
--  e.g., (ij jpred rtab) . (p plist) . (f pred)
runCombinedROp ::
       (RTable -> RTable)  -- ^ input combined RTable operation
    -> RTable              -- ^ input RTable that the input function will be applied to
    -> RTable              -- ^ output RTable
runCombinedROp f rtab = f rtab


-- * ########## RTable DML Operations ##############


-- | O(n) append an RTuple to an RTable
insertAppendRTab :: RTuple -> RTable -> RTable
insertAppendRTab rtup rtab = V.snoc rtab rtup

-- | O(n) prepend an RTuple to an RTable
insertPrependRTab :: RTuple -> RTable -> RTable
insertPrependRTab rtup rtab = V.cons rtup rtab  

-- | Update an RTable. Input includes a list of (ColumnName, new Value) pairs.
-- Also a filter predicate is specified in order to restrict the update only to those
-- rtuples that fulfill the predicate
updateRTab ::
        [(ColumnName, RDataType)] -- ^ List of column names to be updated with the corresponding new values
    ->  RPredicate  -- ^ An RTuple -> Bool function that specifies the RTuples to be updated
    ->  RTable       -- ^ Input RTable
    ->  RTable       -- ^ Output RTable
updateRTab [] _ inputRtab = inputRtab 
updateRTab ((colName, newVal) : rest) rpred inputRtab = 
    {-
        Here is the update algorithm:

        FinalRTable = UNION (Table A) (Table B)

        where
            Table A = the subset of input RTable that includes the rtuples that satisfy the input RPredicate, with updated values in the
                        corresponding columns
            Table B = the subset of input RTable that includes all the rtuples that DONT satisfy the input RPredicate
    -}
    if isRTabEmpty inputRtab 
        then emptyRTable
        else
            let 
                tabA = rtabMap (upsertRTuple colName newVal) (f rpred inputRtab)
                tabB = f (not . rpred) inputRtab 
            in updateRTab rest rpred (u tabA tabB)

-- * ########## RTable IO Operations ##############

-- | Basic data type for defining the desired formatting of an Rtuple
data RTupleFormat = RTupleFormat {
    -- | For defining the column ordering (i.e., the SELECT clause in SQL)
    colSelectList :: [ColumnName]
    -- | For defining the formating per Column (in printf style)
    ,colFormatMap :: ColFormatMap
} deriving (Eq, Show)

-- | A map of ColumnName to Format Specification
type ColFormatMap = HM.HashMap ColumnName FormatSpecifier

-- | Generates a default Column Format Specification
genDefaultColFormatMap :: ColFormatMap
genDefaultColFormatMap = HM.empty

-- | Generates a Column Format Specification
genColFormatMap :: 
    [(ColumnName, FormatSpecifier)]
    -> ColFormatMap
genColFormatMap fs = HM.fromList fs


-- | Format specifier of Text.Printf.printf style.(see https://hackage.haskell.org/package/base-4.10.1.0/docs/Text-Printf.html)
data FormatSpecifier = DefaultFormat | Format String deriving (Eq, Show)

-- | Generate an RTupleFormat data type instance
genRTupleFormat :: 
    [ColumnName]    -- ^ Column Select list 
    -> ColFormatMap -- ^ Column Format Map
    -> RTupleFormat  -- ^ Output
genRTupleFormat colNames colfMap = RTupleFormat { colSelectList = colNames, colFormatMap = colfMap} 

-- | Generate a default RTupleFormat data type instance.
-- In this case the returned column order (Select list), will be unspecified
-- and dependant only by the underlying structure of the RTuple (HashMap)
genRTupleFormatDefault :: RTupleFormat
genRTupleFormatDefault = RTupleFormat { colSelectList = [], colFormatMap = genDefaultColFormatMap }

-- | prints an RTable with an RTuple format specification
printfRTable :: RTupleFormat -> RTable -> IO()
printfRTable rtupFmt rtab = -- undefined
    if isRTabEmpty rtab
        then
            do 
                putStrLn "-------------------------------------------"
                putStrLn " 0 rows returned"
                putStrLn "-------------------------------------------"

        else
            do
                -- find the max value-length for each column, this will be the width of the box for this column
                let listOfLengths = getMaxLengthPerColumnFmt rtupFmt rtab

                --debug
                --putStrLn $ "List of Lengths: " ++ show listOfLengths

                printContLineFmt rtupFmt listOfLengths '-' rtab
                -- print the Header
                printRTableHeaderFmt rtupFmt listOfLengths rtab 

                -- print the body
                printRTabBodyFmt rtupFmt listOfLengths $ V.toList rtab

                -- print number of rows returned
                let numrows = V.length rtab
                if numrows == 1
                    then 
                        putStrLn $ "\n"++ (show $ numrows) ++ " row returned"        
                    else
                        putStrLn $ "\n"++ (show $ numrows) ++ " rows returned"        

                printContLineFmt rtupFmt listOfLengths '-' rtab
                where
                    -- [Int] a List of width per column to be used in the box definition        
                    printRTabBodyFmt :: RTupleFormat -> [Int] -> [RTuple] -> IO()
                    printRTabBodyFmt _ _ [] = putStrLn ""            
                    printRTabBodyFmt rtupf ws (rtup : rest) = do
                            printRTupleFmt rtupf ws rtup
                            printRTabBodyFmt rtupf ws rest


-- | printRTable : Print the input RTable on screen
printRTable ::
       RTable
    -> IO ()
printRTable rtab = 
    if isRTabEmpty rtab
        then
            do 
                putStrLn "-------------------------------------------"
                putStrLn " 0 rows returned"
                putStrLn "-------------------------------------------"

        else
            do
                -- find the max value-length for each column, this will be the width of the box for this column
                let listOfLengths = getMaxLengthPerColumn rtab

                --debug
                --putStrLn $ "List of Lengths: " ++ show listOfLengths

                printContLine listOfLengths '-' rtab
                -- print the Header
                printRTableHeader listOfLengths rtab 

                -- print the body
                printRTabBody listOfLengths $ V.toList rtab

                -- print number of rows returned
                let numrows = V.length rtab
                if numrows == 1
                    then 
                        putStrLn $ "\n"++ (show $ numrows) ++ " row returned"        
                    else
                        putStrLn $ "\n"++ (show $ numrows) ++ " rows returned"        

                printContLine listOfLengths '-' rtab
                where
                    -- [Int] a List of width per column to be used in the box definition        
                    printRTabBody :: [Int] -> [RTuple] -> IO()
                    printRTabBody _ [] = putStrLn ""            
                    printRTabBody ws (rtup : rest) = do
                            printRTuple ws rtup
                            printRTabBody ws rest

-- | Returns the max length of the String representation of each value, for each column of the input RTable. 
-- It returns the lengths in the column order specified by the input RTupleFormat parameter
getMaxLengthPerColumnFmt :: RTupleFormat -> RTable -> [Int]
getMaxLengthPerColumnFmt rtupFmt rtab = 
    let
        -- Create an RTable where all the values of the columns will be the length of the String representations of the original values
        lengthRTab = do
            rtup <- rtab
            let ls = Data.List.map (\(c, v) -> (c, RInt $ fromIntegral $ Data.List.length . rdataTypeToString $ v) ) (rtupleToList rtup)
                -- create an RTuple with the column names lengths
                headerLengths = Data.List.zip (getColumnNamesfromRTab rtab) (Data.List.map (\c -> RInt $ fromIntegral $ Data.List.length c) (getColumnNamesfromRTab rtab))
            -- append  to the rtable also the tuple corresponding to the header (i.e., the values will be the names of the column) in order
            -- to count them also in the width calculation
            (return $ createRtuple ls) V.++ (return $ createRtuple headerLengths)

        -- Get the max length for each column
        resultRTab = findMaxLengthperColumn lengthRTab
                where
                    findMaxLengthperColumn :: RTable -> RTable
                    findMaxLengthperColumn rt = 
                        let colNames = (getColumnNamesfromRTab rt) -- [ColumnName]
                            aggOpsList = Data.List.map (\c -> raggMax c c) colNames  -- [AggOp]
                        in runAggregation aggOpsList rt
         -- get the RTuple with the results
        resultRTuple = headRTup resultRTab
    in 
        -- transform it to [Int]
        if rtupFmt /= genRTupleFormatDefault
            then
                -- generate [Int] in the column order specified by the format parameter        
                Data.List.map (\(RInt i) -> fromIntegral i) $ 
                    Data.List.map (\colname -> resultRTuple <!> colname) $ colSelectList rtupFmt  -- [RInt i]
            else
                -- else just choose the default column order (i.e., unspecified)
                Data.List.map (\(colname, RInt i) -> fromIntegral i) (rtupleToList resultRTuple)                

-- | Returns the max length of the String representation of each value, for each column of the input RTable. 
getMaxLengthPerColumn :: RTable -> [Int]
getMaxLengthPerColumn rtab = 
    let
        -- Create an RTable where all the values of the columns will be the length of the String representations of the original values
        lengthRTab = do
            rtup <- rtab
            let ls = Data.List.map (\(c, v) -> (c, RInt $ fromIntegral $ Data.List.length . rdataTypeToString $ v) ) (rtupleToList rtup)
                -- create an RTuple with the column names lengths
                headerLengths = Data.List.zip (getColumnNamesfromRTab rtab) (Data.List.map (\c -> RInt $ fromIntegral $ Data.List.length c) (getColumnNamesfromRTab rtab))
            -- append  to the rtable also the tuple corresponding to the header (i.e., the values will be the names of the column) in order
            -- to count them also in the width calculation
            (return $ createRtuple ls) V.++ (return $ createRtuple headerLengths)

        -- Get the max length for each column
        resultRTab = findMaxLengthperColumn lengthRTab
                where
                    findMaxLengthperColumn :: RTable -> RTable
                    findMaxLengthperColumn rt = 
                        let colNames = (getColumnNamesfromRTab rt) -- [ColumnName]
                            aggOpsList = Data.List.map (\c -> raggMax c c) colNames  -- [AggOp]
                        in runAggregation aggOpsList rt

{-
        -- create a Julius expression to evaluate the max length per column
        julexpr =  EtlMapStart
                -- Turn each value to an (RInt i) that correposnd to the length of the String representation of the value
                :-> (EtlC $ 
                        Source (getColumnNamesfromRTab rtab)
                        Target (getColumnNamesfromRTab rtab)
                        By (\[value] -> [RInt $ Data.List.length . rdataTypeToString $ value] )
                        (On $ Tab rtab) 
                        RemoveSrc $
                        FilterBy (\rtuple -> True)
                    )
                -- Get the max length for each column
                :-> (EtlR $
                        ROpStart 
                        :. (GenUnaryOp (On Previous) $ ByUnaryOp findMaxLengthperColumn)
                    )
                where
                    findMaxLengthperColumn :: RTable -> RTable
                    findMaxLengthperColumn rt = 
                        let colNames = (getColumnNamesfromRTab rt) -- [ColumnName]
                            aggOpsList = V.map (\c -> raggMax c c) colNames  -- [AggOp]
                        in runAggregation aggOpsList rt

        -- evaluate the xpression and get the result in an RTable
        resultRTab = juliusToRTable julexpr        
-}       
         -- get the RTuple with the results
        resultRTuple = headRTup resultRTab
    in 
        -- transform it to a [Int]
        Data.List.map (\(colname, RInt i) -> fromIntegral i) (rtupleToList resultRTuple)

spaceSeparatorWidth :: Int
spaceSeparatorWidth = 5

-- | helper function in order to format the value of a column
-- It will append at the end of the string n number of spaces.
addSpace :: 
        Int -- ^ number of spaces to add
    ->  String -- ^ input String
    ->  String -- ^ output string
addSpace i s = s ++ Data.List.take i (repeat ' ')    


-- | helper function in order to format the value of a column
-- It will append at the end of the string n number of spaces.
addCharacter :: 
        Int -- ^ number of spaces to add
    ->  Char   -- ^ character to add
    ->  String -- ^ input String
    ->  String -- ^ output string
addCharacter i c s = s ++ Data.List.take i (repeat c)    

-- | helper function that prints a continuous line adjusted to the size of the input RTable
-- The column order is specified by the input RTupleFormat parameter
printContLineFmt ::
       RTupleFormat -- ^ Specifies the appropriate column order
    -> [Int]    -- ^ a List of width per column to be used in the box definition
    -> Char     -- ^ the char with which the line will be drawn
    -> RTable
    -> IO ()
printContLineFmt rtupFmt widths ch rtab = do
    let listOfColNames =    if rtupFmt /= genRTupleFormatDefault
                                then
                                    colSelectList rtupFmt
                                else
                                    getColumnNamesfromRTab rtab -- [ColumnName] 
        listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat ch)) listOfColNames
        formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) ch l) (Data.List.zip widths listOfLinesCont)
        formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont
    putStrLn formattedRowOfLinesCont


-- | helper function that prints a continuous line adjusted to the size of the input RTable
printContLine ::
       [Int]    -- ^ a List of width per column to be used in the box definition
    -> Char     -- ^ the char with which the line will be drawn
    -> RTable
    -> IO ()
printContLine widths ch rtab = do
    let listOfColNames =  getColumnNamesfromRTab rtab -- [ColumnName] 
        listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat ch)) listOfColNames
        formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) ch l) (Data.List.zip widths listOfLinesCont)
        formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont
    putStrLn formattedRowOfLinesCont


-- | Prints the input RTable's header (i.e., column names) on screen.
-- The column order is specified by the corresponding RTupleFormat parameter.
printRTableHeaderFmt ::
       RTupleFormat -- ^ Specifies Column order
    -> [Int]    -- ^ a List of width per column to be used in the box definition
    -> RTable
    -> IO ()
printRTableHeaderFmt rtupFmt widths rtab = do -- undefined    
        let listOfColNames = if rtupFmt /= genRTupleFormatDefault then colSelectList rtupFmt else  getColumnNamesfromRTab rtab -- [ColumnName]
            -- format each column name according the input width and return a list of Boxes [Box]
            -- formattedList =  Data.List.map (\(w,c) -> BX.para BX.left (w + spaceSeparatorWidth) c) (Data.List.zip widths listOfColNames)    -- listOfColNames   -- map (\c -> BX.render . BX.text $ c) listOfColNames
            formattedList = Data.List.map (\(w,c) -> addSpace (w - (Data.List.length c) + spaceSeparatorWidth) c) (Data.List.zip widths listOfColNames) 
            -- Paste all boxes together horizontally
            -- formattedRow = BX.render $ Data.List.foldr (\colname_box accum -> accum BX.<+> colname_box) BX.nullBox formattedList
            formattedRow = Data.List.foldr (\colname accum -> colname ++ accum) "" formattedList
            
            listOfLines = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat '~')) listOfColNames
          --  listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat '-')) listOfColNames
            --formattedLines = Data.List.map (\(w,l) -> BX.para BX.left (w +spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
            formattedLines = Data.List.map (\(w,l) -> addSpace (w - (Data.List.length l) + spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
          -- formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) '-' l) (Data.List.zip widths listOfLinesCont)
            -- formattedRowOfLines = BX.render $ Data.List.foldr (\line_box accum -> accum BX.<+> line_box) BX.nullBox formattedLines
            formattedRowOfLines = Data.List.foldr (\line accum -> line ++ accum) "" formattedLines
          ---  formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont

      --  printUnderlines formattedRowOfLinesCont
        printHeader formattedRow
        printUnderlines formattedRowOfLines
        where
            printHeader :: String -> IO()
            printHeader h = putStrLn h

            printUnderlines :: String -> IO()
            printUnderlines l = putStrLn l

-- | printRTableHeader : Prints the input RTable's header (i.e., column names) on screen
printRTableHeader ::
       [Int]    -- ^ a List of width per column to be used in the box definition
    -> RTable
    -> IO ()
printRTableHeader widths rtab = do -- undefined    
        let listOfColNames =  getColumnNamesfromRTab rtab -- [ColumnName]
            -- format each column name according the input width and return a list of Boxes [Box]
            -- formattedList =  Data.List.map (\(w,c) -> BX.para BX.left (w + spaceSeparatorWidth) c) (Data.List.zip widths listOfColNames)    -- listOfColNames   -- map (\c -> BX.render . BX.text $ c) listOfColNames
            formattedList = Data.List.map (\(w,c) -> addSpace (w - (Data.List.length c) + spaceSeparatorWidth) c) (Data.List.zip widths listOfColNames) 
            -- Paste all boxes together horizontally
            -- formattedRow = BX.render $ Data.List.foldr (\colname_box accum -> accum BX.<+> colname_box) BX.nullBox formattedList
            formattedRow = Data.List.foldr (\colname accum -> colname ++ accum) "" formattedList
            
            listOfLines = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat '~')) listOfColNames
          --  listOfLinesCont = Data.List.map (\c -> Data.List.take (Data.List.length c) (repeat '-')) listOfColNames
            --formattedLines = Data.List.map (\(w,l) -> BX.para BX.left (w +spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
            formattedLines = Data.List.map (\(w,l) -> addSpace (w - (Data.List.length l) + spaceSeparatorWidth) l) (Data.List.zip widths listOfLines)
          -- formattedLinesCont = Data.List.map (\(w,l) -> addCharacter (w - (Data.List.length l) + spaceSeparatorWidth) '-' l) (Data.List.zip widths listOfLinesCont)
            -- formattedRowOfLines = BX.render $ Data.List.foldr (\line_box accum -> accum BX.<+> line_box) BX.nullBox formattedLines
            formattedRowOfLines = Data.List.foldr (\line accum -> line ++ accum) "" formattedLines
          ---  formattedRowOfLinesCont = Data.List.foldr (\line accum -> line ++ accum) "" formattedLinesCont

      --  printUnderlines formattedRowOfLinesCont
        printHeader formattedRow
        printUnderlines formattedRowOfLines
        where
            printHeader :: String -> IO()
            printHeader h = putStrLn h

            printUnderlines :: String -> IO()
            printUnderlines l = putStrLn l
{-          printHeader :: [String] -> IO ()
            printHeader [] = putStrLn ""
            printHeader (x:xs) = do
                    putStr $ x ++ "\t"
                    printHeader xs
            
            printUnderlines :: [String] -> IO ()
            printUnderlines [] = putStrLn ""
            printUnderlines (x:xs) = do
                putStr $ (Data.List.take (Data.List.length x) (repeat '~')) ++ "\t" 
                printUnderlines xs
-}

-- | Prints an RTuple on screen (only the values of the columns)
--  [Int] is a List of width per column to be used in the box definition        
-- The column order as well as the formatting specifications are specified by the first parameter.
-- We assume that the order in [Int] corresponds to that of the RTupleFormat parameter.
printRTupleFmt :: RTupleFormat -> [Int] -> RTuple -> IO()
printRTupleFmt rtupFmt widths rtup = do
    -- take list of values of each column and convert to String
    let rtupList =  if rtupFmt == genRTupleFormatDefault
                        then -- then no column ordering is specified nore column formatting
                            Data.List.map (rdataTypeToString . snd) (rtupleToList rtup)  -- [RDataType] --> [String]
                        else 
                            if (colFormatMap rtupFmt) == genDefaultColFormatMap
                                then -- col ordering is specified but no formatting per column
                                    Data.List.map (\colname -> rdataTypeToStringFmt DefaultFormat $ rtup <!> colname ) $ colSelectList rtupFmt
                                else  -- both column ordering, as well as formatting per column is specified
                                    Data.List.map (\colname -> rdataTypeToStringFmt ((colFormatMap rtupFmt) ! colname) $ rtup <!> colname ) $ colSelectList rtupFmt

        -- format each column value according the input width and return a list of [Box]
        -- formattedValueList = Data.List.map (\(w,v) -> BX.para BX.left w v) (Data.List.zip widths rtupList)
        formattedValueList = Data.List.map (\(w,v) -> addSpace (w - (Data.List.length v) + spaceSeparatorWidth) v) (Data.List.zip widths rtupList)
                        -- Data.List.map (\(c,r) -> BX.text . rdataTypeToString $ r) rtupList 
                        -- Data.List.map (\(c,r) -> rdataTypeToString $ r) rtupList --  -- [String]
        -- Paste all boxes together horizontally
        -- formattedRow = BX.render $ Data.List.foldr (\value_box accum -> accum BX.<+> value_box) BX.nullBox formattedValueList
        formattedRow = Data.List.foldr (\value_box accum -> value_box ++ accum) "" formattedValueList
    putStrLn formattedRow

-- | Prints an RTuple on screen (only the values of the columns)
--  [Int] is a List of width per column to be used in the box definition        
printRTuple :: [Int] -> RTuple -> IO()
printRTuple widths rtup = do
    -- take list of values of each column and convert to String
    let rtupList = Data.List.map (rdataTypeToString . snd) (rtupleToList rtup)  -- [RDataType] --> [String]

        -- format each column value according the input width and return a list of [Box]
        -- formattedValueList = Data.List.map (\(w,v) -> BX.para BX.left w v) (Data.List.zip widths rtupList)
        formattedValueList = Data.List.map (\(w,v) -> addSpace (w - (Data.List.length v) + spaceSeparatorWidth) v) (Data.List.zip widths rtupList)
                        -- Data.List.map (\(c,r) -> BX.text . rdataTypeToString $ r) rtupList 
                        -- Data.List.map (\(c,r) -> rdataTypeToString $ r) rtupList --  -- [String]
        -- Paste all boxes together horizontally
        -- formattedRow = BX.render $ Data.List.foldr (\value_box accum -> accum BX.<+> value_box) BX.nullBox formattedValueList
        formattedRow = Data.List.foldr (\value_box accum -> value_box ++ accum) "" formattedValueList
    putStrLn formattedRow

{-    printList formattedValueList
    where 
        printList :: [String] -> IO()
        printList [] = putStrLn ""
        printList (x:xs) = do
            putStr $ x ++ "\t"
            printList xs
-}

-- | Turn the value stored in a RDataType into a String in order to be able to print it wrt to the specified format
rdataTypeToStringFmt :: FormatSpecifier -> RDataType -> String
rdataTypeToStringFmt fmt rdt =
    case fmt of 
        DefaultFormat -> rdataTypeToString rdt
        Format fspec -> 
            case rdt of
                RInt i -> printf fspec i
                RText t -> printf fspec (unpack t)
                RDate {rdate = d, dtformat = f} -> printf fspec (unpack d)
                RTime t -> printf fspec $ unpack $ rtext (rTimeStampToRText "DD/MM/YYYY HH24:MI:SS" t)
                -- Round to only two decimal digits after the decimal point
                RDouble db -> printf fspec db -- show db
                Null -> "NULL"           

-- | Turn the value stored in a RDataType into a String in order to be able to print it
-- Values are transformed with a default formatting. 
rdataTypeToString :: RDataType -> String
rdataTypeToString rdt =
    case rdt of
        RInt i -> show i
        RText t -> unpack t
        RDate {rdate = d, dtformat = f} -> unpack d
        RTime t -> unpack $ rtext (rTimeStampToRText "DD/MM/YYYY HH24:MI:SS" t)
        -- Round to only two decimal digits after the decimal point
        RDouble db -> printf "%.2f" db -- show db
        Null -> "NULL"



{-

-- | This data type is used in order to be able to print the value of a column of an RTuple
data ColPrint = ColPrint { colName :: String, val :: String } deriving (Data, G.Generic)
instance PP.Tabulate ColPrint

data PrintableRTuple = PrintableRTuple [ColPrint]  deriving (Data, G.Generic)
instance PP.Tabulate PrintableRTuple

instance PP.CellValueFormatter PrintableRTuple where
    -- ppFormatter :: a -> String
    ppFormatter [] = ""
    ppFormatter (colpr:rest)  = (BX.render $ BX.text (val colpr)) ++ "\t" ++ ppFormatter rest

-- | Turn an RTuple to a list of RTuplePrint
rtupToPrintableRTup :: RTuple -> PrintableRTuple
rtupToPrintableRTup rtup = 
    let rtupList = rtupleToList rtup  -- [(ColumnName, RDataType)]
    in PrintableRTuple $ Data.List.map (\(c,r) -> ColPrint { colName = c, val = rdataTypeToString r }) rtupList  -- [ColPrint]

-- | Turn the value stored in a RDataType into a String in order to be able to print it
rdataTypeToString :: RDataType -> String
rdataTypeToString rdt = undefined

-- | printRTable : Print the input RTable on screen
printRTable ::
       RTable
    -> IO ()
printRTable rtab = -- undefined
    do 
        let vectorOfprintableRTups = do 
                                rtup <- rtab
                                let rtupPrint = rtupToPrintableRTup rtup 
                                return rtupPrint
{-

                                let rtupList = rtupleToList rtup  -- [(ColumnName, RDataType)]
                                    colNamesList = Data.List.map (show . fst) rtupList  -- [String]
                                    rdatatypesStringfied = Data.List.map (rdataTypeToString . snd) rtupList  -- [String]
                                    map = Data.Map.fromList $  Data.List.zip colNamesList rdatatypesStringfied -- [(String, String)]                                
                                return colNamesList -- map -}
        PP.ppTable vectorOfprintableRTups

-}