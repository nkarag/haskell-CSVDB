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
        ,RPredicate (..)
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
        ,emptyRTable
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
        --,runCombinedROp 
        ,rcomb
        ,removeColumn
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
import Data.List (map, zip, elemIndex, sortOn, union, intersect, (\\), take, length)
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


-- | getRTupColValue :: Returns the value of an RTuple column based on the ColumnName key
--   if the column name is not found, then it returns Null
getRTupColValue ::
       ColumnName    -- ^ ColumnName key
    -> RTuple        -- ^ Input RTuple
    -> RDataType     -- ^ Output value
getRTupColValue =  HM.lookupDefault Null


-- newtype NumericRDT = NumericRDT { getRDataType :: RDataType } deriving (Eq, Ord, Read, Show, Num)

instance Num RDataType where
    (+) (RInt i1) (RInt i2) = RInt (i1 + i2)
    (*) (RInt i1) (RInt i2) = RInt (i1 * i2)
    abs (RInt i) = RInt (abs i)
    signum (RInt i) = RInt (signum i)
    fromInteger i = RInt i
    negate (RInt i) = RInt (negate i)

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
    in Prelude.map (\cname -> mapColNameColInfo!cname) (toListColumnName rtupmd)


-- | toListRDataType: returns a list of RDataType values of an RTuple, in the fixed column order of the RTuple
toListRDataType :: 
    RTupleMData
    -> RTuple
    -> [RDataType]
toListRDataType rtupmd rtup = Prelude.map (\cname -> rtup!cname) (toListColumnName rtupmd)


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
    | RCombinedOp { rcombOp :: RTable -> RTable  }   -- ^ A combination of unary ROperations e.g.,   (p plist).(f pred)  (i.e., RPrj . RFilter), in the form of an RTable -> RTable function.
                                                     --  In this sense we can also include a binary operation (e.g. join), if we partially apply the join to one RTable
                                                     --  e.g., (ij jpred rtab) . (p plist) . (f pred)
 --deriving (Eq,Show)
    -- | RFieldRenaming
    -- | RAggregation
    -- | RExtension

-- | Definition of a Join Predicate
type RJoinPredicate = RTuple -> RTuple -> Bool

-- | ropU operator executes a unary ROperation
ropU = runUnaryROperation

-- | Execute a Unary ROperation
runUnaryROperation :: 
    ROperation -- ^ input ROperation
    -> RTable  -- ^ input RTable
    -> RTable  -- ^ output RTable
runUnaryROperation rop irtab = 
    case rop of
        RFilter { fpred = rpredicate } ->  runRfilter rpredicate irtab
        RPrj { colPrjList = colNames } ->  runProjection colNames irtab
        RCombinedOp { rcombOp = comb } ->  runCombinedROp comb irtab 


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
-- Map (i.e., RTuple) will be prefered when dublicate keys encountered" that is, in the context of 
-- joining two RTuples the value of the first RTuple on the common key will be prefered.
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

-- | Implements a Left Outer Join operation between two RTables (any type of join predicate is allowed)
-- Note that this operation is implemented as a 'Data.HashMap.Strict' union, which means "the first 
-- Map (i.e., RTuple) will be prefered when dublicate keys encountered" that is, in the context of 
-- joining two RTuples the value of the first RTuple on the common key will be prefered.
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
-- Note that this operation is implemented as a 'Data.HashMap.Strict' union, which means "the first 
-- Map (i.e., RTuple) will be prefered when dublicate keys encountered" that is, in the context of 
-- joining two RTuples the value of the first RTuple on the common key will be prefered.
runRightJoin ::
    RJoinPredicate
    -> RTable
    -> RTable
    -> RTable
runRightJoin jpred irtab1 irtab2 = undefined

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

rcomb = runCombinedROp

-- | runCombinedROp: A Higher Order function that accepts as input a combination of unary ROperations e.g.,   (p plist).(f pred)
--   expressed in the form of a function (RTable -> Rtable) and applies this function to the input RTable.
--  In this sense we can also include a binary operation (e.g. join), if we partially apply the join to one RTable
--  e.g., (ij jpred rtab) . (p plist) . (f pred)
runCombinedROp ::
       (RTable -> RTable)  -- ^ input combined RTable operation
    -> RTable              -- ^ input RTable that the input function will be applied to
    -> RTable              -- ^ output RTable
runCombinedROp f rtab = f rtab