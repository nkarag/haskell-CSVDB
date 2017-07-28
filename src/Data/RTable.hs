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
{-# LANGUAGE RecordWildCards #-}
--  :set -XRecordWildCards
{-# LANGUAGE GeneralizedNewtypeDeriving #-}  -- In order to be able to derive from non-standard derivable classes (such as Num)

-- {-# LANGUAGE  DuplicateRecordFields #-}


module Data.RTable
    ( 
       -- Relation(..)
        RTable (..)
        ,RTuple (..)
        ,RPredicate
        ,RGroupPredicate
        ,RJoinPredicate
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
        ,raggSum
        ,raggCount
        ,raggAvg
        ,raggMax
        ,raggMin
        ,emptyRTable
        ,emptyRTuple
        ,isRTabEmpty
        ,isRTupEmpty
        ,createRDataType
        ,createNullRTuple
        ,createRtuple
        ,isNullRTuple
        ,restrictNrows
        ,createRTableMData
        ,createRTimeStamp
        ,getRTupColValue
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
        --,runRfilter
        ,f
        --,runInnerJoin
        ,iJ
        --,runLeftJoin
        ,lJ
        --,runRightJoin
        ,rJ
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
        --,runCombinedROp 
        ,rComb
        ,removeColumn
        ,addColumn
        ,nvl
    ) where

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
import Data.List (map, zip, zipWith, elemIndex, sortOn, union, intersect, (\\), take, length, groupBy, sortBy, foldl', foldr, foldr1, foldl',head)
-- Data.Maybe
import Data.Maybe (fromJust)
-- Data.Char
import Data.Char (toUpper,digitToInt)

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


-- | Definitioin of the Name type
type Name = String
-- | Definition of the Column Name
type ColumnName = Name
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
      deriving (Show, Eq, TB.Typeable, Ord, Read)   -- http://stackoverflow.com/questions/6600380/what-is-haskells-data-typeable

-- | Standard date format
stdDateFormat = "DD/MM/YYYY"

-- | Get the first RTuple from an RTable
headRTup ::
        RTable
    ->  RTuple
headRTup = V.head    


-- | getRTupColValue :: Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns Null
getRTupColValue ::
       ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> RDataType     -- ^ Output value
getRTupColValue =  HM.lookupDefault Null

-- | Operator for getting a column value from an RTuple
--   if the column name is not found, then it returns Null
(<!>) ::
       RTuple        -- ^ Input RTuple
    -> ColumnName    -- ^ ColumnName key
    -> RDataType     -- ^ Output value
(<!>) = flip getRTupColValue


-- | Returns the 1st parameter if this is not Null, otherwise it returns the 2nd. 
nvl ::
       RDataType
    -> RDataType
    -> RDataType 
nvl v1 v2 = 
    if v1 == Null
        then v2
        else v1

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

instance Num RDataType where
    (+) (RInt i1) (RInt i2) = RInt (i1 + i2)
    (+) (RDouble d1) (RDouble d2) = RDouble (d1 + d2)
    (+) (RDouble d1) (RInt i2) = RDouble (d1 + fromIntegral i2)
    (+) (RInt i1) (RDouble d2) = RDouble (fromIntegral i1 + d2)
    (+) (RInt i1) (Null) = RInt i1  -- ignore Null - just like in SQL
    (+) (Null) (RInt i2) = RInt i2  -- ignore Null - just like in SQL
    (+) (RDouble d1) (Null) = RDouble d1  -- ignore Null - just like in SQL
    (+) (Null) (RDouble d2) = RDouble d2  -- ignore Null - just like in SQL    
    (+) _ _ = Null
    (*) (RInt i1) (RInt i2) = RInt (i1 * i2)
    (*) (RDouble d1) (RDouble d2) = RDouble (d1 * d2)
    (*) (RDouble d1) (RInt i2) = RDouble (d1 * fromIntegral i2)
    (*) (RInt i1) (RDouble d2) = RDouble (fromIntegral i1 * d2)
    (*) (RInt i1) (Null) = RInt i1  -- ignore Null - just like in SQL
    (*) (Null) (RInt i2) = RInt i2  -- ignore Null - just like in SQL
    (*) (RDouble d1) (Null) = RDouble d1  -- ignore Null - just like in SQL
    (*) (Null) (RDouble d2) = RDouble d2  -- ignore Null - just like in SQL    
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
rTimeStampToRText stdTimestampFormat ts = let timeString = show (day ts) ++ "/" ++ show (month ts) ++ "/" ++ show (year ts) ++ " " ++ show (hours24 ts) ++ ":" ++ show (minutes ts) ++ ":" ++ show (seconds ts)
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
      RUnion   -- ^ Union 
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
    | RCombinedOp { rcombOp :: RTable -> RTable  }   -- ^ A combination of unary ROperations e.g.,   (p plist).(f pred)  (i.e., RPrj . RFilter), in the form of an RTable -> RTable function.
                                                     --  In this sense we can also include a binary operation (e.g. join), if we partially apply the join to one RTable
                                                     --  e.g., (ij jpred rtab) . (p plist) . (f pred)
    | RBinOp { rbinOp :: RTable -> RTable -> RTable } -- ^ A generic binary ROperation.


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
                    if (getRTupColValue src) rtup /= Null && accValue /= Null
                        then
                            (getRTupColValue src) rtup + accValue
                        else
                            if (getRTupColValue src) rtup == Null && accValue /= Null 
                                then
                                    accValue  -- ignore Null value
                                else
                                    if (getRTupColValue src) rtup /= Null && accValue == Null 
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
                            if (getRTupColValue src) rtup /= Null && accValue /= Null
                                then
                                    RInt 1 + accValue
                                else
                                    if (getRTupColValue src) rtup == Null && accValue /= Null 
                                        then
                                            accValue  -- ignore Null value
                                        else
                                            if (getRTupColValue src) rtup /= Null && accValue == Null 
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
                                if (getRTupColValue src) rtup /= Null && accValue /= Null
                                    then
                                        max ((getRTupColValue src) rtup) accValue
                                    else
                                        if (getRTupColValue src) rtup == Null && accValue /= Null 
                                            then
                                                accValue  -- ignore Null value
                                            else
                                                if (getRTupColValue src) rtup /= Null && accValue == Null 
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
                                if (getRTupColValue src) rtup /= Null && accValue /= Null
                                    then
                                        min ((getRTupColValue src) rtup) accValue
                                    else
                                        if (getRTupColValue src) rtup == Null && accValue /= Null 
                                            then
                                                accValue  -- ignore Null value
                                            else
                                                if (getRTupColValue src) rtup /= Null && accValue == Null 
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
        checkt = HM.filter (\v -> v /= Null) t 
    in if isRTupEmpty checkt 
            then True
            else False


-- * ########## RTable Operations ##############

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
runRfilter = V.filter

-- | RTable Projection operator
p = runProjection

-- | Implements RTable projection operation
runProjection :: 
    [ColumnName]  -- ^ list of column names to be included in the final result RTable
    -> RTable
    -> RTable
runProjection colNamList irtab = do -- RTable is a Monad
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
iJ = runInnerJoin

-- | Implements an Inner Join operation between two RTables (any type of join predicate is allowed)
-- Note that this operation is implemented as a 'Data.HashMap.Strict' union, which means "the first 
-- Map (i.e., the left RTuple) will be prefered when dublicate keys encountered with different values. That is, in the context of 
-- joining two RTuples the value of the first (i.e., left) RTuple on the common key will be prefered.
runInnerJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runInnerJoin jpred irtab1 irtab2 =  do
    rtup1 <- irtab1
    rtup2 <- irtab2
    let targetRtuple = 
            if (jpred rtup1 rtup2)
            then HM.union rtup1 rtup2                 
            else HM.empty
    removeEmptyRTuples (return targetRtuple)
        where removeEmptyRTuples = f (not.isRTupEmpty) 

-- | RTable Left Outer Join Operator
lJ = runLeftJoin

-- | Implements a Left Outer Join operation between two RTables (any type of join predicate is allowed),
-- i.e., the rows of the left RTable will be preserved.
-- Note that when dublicate keys encountered that is, since the underlying structure for an RTuple is a Data.HashMap.Strict,
-- only one value per key is allowed. So in the context of joining two RTuples the value of the left RTuple on the common key will be prefered.
runLeftJoin ::
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
    return targetRtuple

-- | RTable Right Outer Join Operator
rJ = runRightJoin

-- | Implements a Right Outer Join operation between two RTables (any type of join predicate is allowed)
-- i.e., the rows of the right RTable will be preserved.
-- Note that when dublicate keys encountered that is, since the underlying structure for an RTuple is a Data.HashMap.Strict,
-- only one value per key is allowed. So in the context of joining two RTuples the value of the right RTuple on the common key will be prefered.
runRightJoin ::
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
    return targetRtuple

-- | RTable Union Operator
u = runUnion

-- | Implements the union of two RTables as a union of two lists (see 'Data.List').
-- Duplicates, and elements of the first list, are removed from the the second list, but if the first list contains duplicates, so will the result
runUnion :: 
    RTable
    -> RTable
    -> RTable
runUnion rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = Data.List.union ls1 ls2
    in  V.fromList resultLs

-- | RTable Intersection Operator
i = runIntersect

-- | Implements the intersection of two RTables as an intersection of two lists (see 'Data.List').
runIntersect :: 
    RTable
    -> RTable
    -> RTable
runIntersect rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = Data.List.intersect ls1 ls2
    in  V.fromList resultLs

-- | RTable Difference Operator
d = runDiff

-- | Implements the set Difference of two RTables as the diff of two lists (see 'Data.List').
runDiff :: 
    RTable
    -> RTable
    -> RTable
runDiff rt1 rt2 = 
    let ls1 = V.toList rt1
        ls2 = V.toList rt2
        resultLs = ls1 Data.List.\\ ls2
    in  V.fromList resultLs


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
    let rtupList = V.toList rtab
        -- 1. form the groups of RTuples
            -- a. first sort the Rtuples based on the grouping predicate
        listOfRTupSorted = Data.List.sortBy (\t1 t2 -> if (gpred t1 t2) then EQ else compare (HM.toList t1) (HM.toList t2) ) rtupList --(t1!(Data.List.head . HM.keys $ t1)) (t2!(Data.List.head . HM.keys $ t2)) ) rtupList
            -- b then produce the groups
        listOfRTupGroupLists = Data.List.groupBy gpred listOfRTupSorted
        -- 2. turn each (sub)list of Rtuples representing a Group into an RTable in order to apply aggregation
        --    Note: each RTable in this list will hold a group of RTuples that all have the same values in the input grouping columns
        --    (which must be compatible with the input grouping predicate)
        listofGroupRtabs = Data.List.map (V.fromList) listOfRTupGroupLists
        -- 3. We need to keep the values of the grouping columns (e.g., by selecting the fisrt row) from each one of these RTables,
        --    in order to "join them" with the aggregated RTuples that will be produced by the aggregation operations
        --    The following will produce a list of singleton RTables.
        listOfGroupingColumnsRtabs = Data.List.map ( (restrictNrows 1) . (p cols) ) listofGroupRtabs
        -- 4. Aggregate each group according to input and produce a list of (aggregated singleton RTables)
        listOfAggregatedRtabs = Data.List.map (rAgg aggOps) listofGroupRtabs
        -- 5. Join the two list of singleton RTables
        listOfFinalRtabs = Data.List.zipWith (iJ (\t1 t2 -> True)) listOfGroupingColumnsRtabs listOfAggregatedRtabs
        -- 6. Union all individual singleton RTables into the final RTable
    in  Data.List.foldr1 (u) listOfFinalRtabs


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