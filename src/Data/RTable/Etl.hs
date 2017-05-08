{-|
Module      : ETL
Description : Implements ETL operations over RTables.
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
--{-# LANGUAGE OverloadedRecordFields #-}
--{-# LANGUAGE  DuplicateRecordFields #-}

module Data.RTable.Etl 
    (
        RColMapping (..)
        --,runColMapping
        ,runCM
        ,YesNo (..)
        --,RTabMapping
        --,runTabMapping
        --,t
        ,ETLOperation (..)
        ,etlOpU
        ,etlOpB
        ,ETLMapping
        --,runETLmapping
        ,etl
        ,rdtappend        
    ) where 

-- Data.RTable
import Data.RTable

-- Text
import Data.Text as T

-- HashMap                          -- https://hackage.haskell.org/package/unordered-containers-0.2.7.2/docs/Data-HashMap-Strict.html
import Data.HashMap.Strict as HM

-- Data.List
import Data.List (notElem, map, zip)

data YesNo = Yes | No deriving (Eq, Show)

-- | RColMapping: This is the basic data type to define the column-to-column mapping from a source RTable to a target RTable.
--   Essentially, an RColMapping represents the column-level transformations of an Rtuple that will yield a target RTuple. 
--
--   A mapping is simply a triple of the form ( Source-Column(s), Target-Column(s), Transformation, RTuple-Filter), where we define the source columns
--   over which a transformation (i.e. a function) will be applied in order to yield the target columns. Also, an RPredicate (i.e. a filter) might be applied on the source RTuple.
--   Remember that an RTuple is essentially a mapping between a key (the Column Name) and a value (the RDataType value). So the various RColMapping
--   data constructors below simply describe the possible modifications of an RTuple orginating from its own columns.
--
--   So, we can have the following mapping types:
--          a) single-source column to single-target column mapping (1 to 1), 
--                  the source column will be removed or not based on the removeSrcCol flag (dublicate column names are not allowed in an RTuple)
--          b) multiple-source columns to single-target column mapping (N to 1),
--                  The N columns will be merged  to the single target column based on the transformation.
--                  The N columns will be removed from the RTuple or not based on the removeSrcCol flag (dublicate column names are not allowed in an RTuple)
--          c) single-source column to multiple-target columns mapping  (1 to M)
--                  the source column will be "expanded" to M target columns based ont he transformation.
--                  the source column will be removed or not based on the removeSrcCol flag (dublicate column names are not allowed in an RTuple)                  
--          d) multiple-source column to multiple target columns mapping (N to M)
--                  The N source columns will be mapped to M target columns based on the transformation.
--                  The N columns will be removed from the RTuple or not based on the removeSrcCol flag (dublicate column names are not allow in an RTuple)
--
--   Some examples of mapping are the following:
--   @
--      ("Start_Date", No, "StartDate", \t -> True)  --  copy the source value to target and dont remove the source column, so the target RTuple will have both columns "Start_Date" and "StartDate"
--                                           --  with the exactly the same value)
--
--      (["Amount", "Discount"], Yes, "FinalAmount", (\[a, d] -> a * d) ) -- "FinalAmount" is a derived column based on a function applied to the two source columns. 
--                                                                        --  In the final RTuple we remove the two source columns.
--   @
--
--  An RColMapping can be applied with the runCM (runColMapping) operator
--
data RColMapping = 
        ColMapEmpty
    |   RMap1x1 { srcCol :: ColumnName,         removeSrcCol :: YesNo,  trgCol :: ColumnName,       transform1x1 :: RDataType    -> RDataType,   srcRTupleFilter:: RPredicate   }   -- ^ single-source column to single-target column mapping (1 to 1).
    |   RMapNx1 { srcColGrp :: [ColumnName],    removeSrcCol :: YesNo,  trgCol :: ColumnName,       transformNx1 :: [RDataType]  -> RDataType,   srcRTupleFilter:: RPredicate   }      -- ^ multiple-source columns to single-target column mapping (N to 1)
    |   RMap1xN { srcCol :: ColumnName,         removeSrcCol :: YesNo,  trgColGrp :: [ColumnName],  transform1xN :: RDataType    -> [RDataType], srcRTupleFilter:: RPredicate }    -- ^ single-source column to multiple-target columns mapping (1 to N)
    |   RMapNxM { srcColGrp :: [ColumnName],    removeSrcCol :: YesNo,  trgColGrp :: [ColumnName],  transformNxM :: [RDataType]  -> [RDataType], srcRTupleFilter:: RPredicate }    -- ^ multiple-source column to multiple target columns mapping (N to M)                                                 



-- | runCM operator executes an RColMapping
runCM = runColMapping

-- | Apply an RColMapping to a source RTable and produce a new RTable.
runColMapping :: RColMapping -> RTable -> RTable
runColMapping ColMapEmpty rtabS = rtabS
runColMapping rmap rtabS = 
    case rmap of 
        RMap1x1 {srcCol = src, trgCol = trg, removeSrcCol = rmvFlag, transform1x1 = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                srcRtuple <- f pred rtabS                                                                        
                let 
                    -- 1. get original column value 
                    srcValue = getRTupColValue src srcRtuple
                    -- srcValue = HM.lookupDefault    Null -- return Null if value cannot be found based on column name 
                    --                                src   -- column name to look for (source) - i.e., the key in the HashMap
                    --                                srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                    
                    -- 2. apply transformation to retrieve new column value
                    trgValue = xform srcValue                                         
                    
                    -- 3. remove the original ColumnName, Value mapping from the RTuple
                    rtupleTemp = 
                        case rmvFlag of
                            Yes -> HM.delete src srcRtuple
                            No  -> srcRtuple
                    
                    -- 4. insert new (ColumnName, Value) pair and thus create the target RTuple
                    trgRtuple = HM.insert trg trgValue rtupleTemp
                
                -- return new RTable
                return trgRtuple

        RMapNx1 {srcColGrp = srcL, trgCol = trg, removeSrcCol = rmvFlag, transformNx1 = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                srcRtuple <- f pred rtabS                                                                        
                let 
                    -- 1. get original column value (in this case it is a list of values)
                    srcValueL = Data.List.map ( \src ->  getRTupColValue src srcRtuple

                                    -- \src -> HM.lookupDefault       Null -- return Null if value cannot be found based on column name 
                                    --                                 src   -- column name to look for (source) - i.e., the key in the HashMap
                                    --                                 srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                                    ) srcL
                    
                    -- 2. apply transformation to retrieve new column value
                    trgValue = xform srcValueL                                         
                    
                    -- 3. remove the original (ColumnName, Value) mappings from the RTuple (i.e., remove ColumnNames mentioned in the RColMapping from source RTuple)
                    rtupleTemp = 
                        case rmvFlag of
                            Yes -> HM.filterWithKey (\colName _ -> notElem colName srcL) srcRtuple
                            No  -> srcRtuple
                    
                    -- 4. insert new ColumnName, Value mapping as the target RTuple must be
                    trgRtuple = HM.insert trg trgValue rtupleTemp
                -- return new RTable
                return trgRtuple

        RMap1xN {srcCol = src, trgColGrp = trgL, removeSrcCol = rmvFlag, transform1xN = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                srcRtuple <- f pred rtabS                                                                        
                let 
                    -- 1. get original column value 
                    srcValue = getRTupColValue src srcRtuple

                    -- srcValue = HM.lookupDefault    Null -- return Null if value cannot be found based on column name 
                    --                                src   -- column name to look for (source) - i.e., the key in the HashMap
                    --                                srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)

                    -- 2. apply transformation to retrieve new column value list
                    trgValueL = xform srcValue                                         

                    -- 3. remove the original ColumnName, Value mapping from the RTuple
                    rtupleTemp = 
                        case rmvFlag of
                            Yes -> HM.delete src srcRtuple
                            No  -> srcRtuple

                    -- 4. insert new (ColumnName, Value) pairs to the target RTuple
                    tempL = Data.List.zip trgL trgValueL
                    trgRtuple = HM.union (HM.fromList tempL) rtupleTemp  -- implement as a hashmap union between new (columnName,value) pairs and source tuple
                        
                -- return new RTable
                return trgRtuple

        RMapNxM {srcColGrp = srcL, trgColGrp = trgL, removeSrcCol = rmvFlag, transformNxM = xform, srcRTupleFilter = pred} -> do  -- an RTable is a Monad just like a list is a Monad, representing a non-deterministic value
                srcRtuple <- f pred rtabS                                                                        
                let 
                    -- 1. get original column value (in this case it is a list of values)
                    srcValueL = Data.List.map (  \src ->  getRTupColValue src srcRtuple

                                    -- \src -> HM.lookupDefault       Null -- return Null if value cannot be found based on column name 
                                    --                                 src   -- column name to look for (source) - i.e., the key in the HashMap
                                    --                                 srcRtuple  -- source RTuple (i.e., a HashMap ColumnName RDataType)
                                    ) srcL
                    
                    -- 2. apply transformation to retrieve new column value
                    trgValueL = xform srcValueL                                         

                    -- 3. remove the original ColumnName, Value mapping from the RTuple
                    rtupleTemp = 
                        case rmvFlag of
                            Yes -> HM.filterWithKey (\colName _ -> notElem colName srcL) srcRtuple
                            No  -> srcRtuple

                    -- 4. insert new (ColumnName, Value) pairs to the target RTuple
                    tempL = Data.List.zip trgL trgValueL
                    trgRtuple = HM.union (HM.fromList tempL) rtupleTemp  -- implement as a hashmap union between new (columnName,value) pairs and source tuple
                        
                -- return new RTable
                return trgRtuple


-- | An RTabMapping is simply a series of RColMappings applied on by one in a fixed order, of the form:
--   RColMapping3 :-> RColMapping2 :-> RColMapping1 Rtab, which  is equivalent to  (RColMapping3 :-> (RColMapping2 :-> (RColMapping1 Rtab)))
--   I.e., the (:->) operator is right associative just like function composition (.).
--   
--   This data type represents a seires of column-based transformation applied to an RTable in order to generate a new RTable.
--
--   An RTabMapping can be applied with the t (rtabTortab) operator.
{--infixr 9 :->
data RTabMapping = ColMapEmptyping | RColMapping :-> RTabMapping
--}

--type RTabMapping = [RColMapping] 

-- | t operator executes an RTabMapping
{--
t = runTabMapping
--}

-- | Execute basic RTable to Rtable mapping.
--   It returns a new RTable (target) which originates from a (source) RTable
--   based on a detailed column-to-column list of mappings.
--
{--
runTabMapping ::
    RTabMapping    -- ^ a series of mappings describing how each value of each column of the target table will be generated based on source columns and transformations
    -> RTable            -- ^ Source RTable
    -> RTable            -- ^ Target RTable
runTabMapping EmptyColMapping rtabSource      = rtabSource  
runTabMapping (colMap :-> tabMap) rtabSource  = 
            let currentRtab = runTabMapping tabMap rtabSource  
            in  runCM colMap currentRtab 
--}

--   The basic assumption here is that in order to produce the target RTable, we need to apply
--   each one of the transformations embedded in the list of input RColMappings, ***from left to right, one by one, starting from the source RTable***. 
--   Each mapping in the list receives as input the output RTable of the previous mapping. This means that if two mappings affect the same column of an RTable, 
--   then the righmost transformation will be the last to change the column value.
--
{--runTabMapping ::
    RTabMapping    -- ^ a series of mappings describing how each value of each column of the target table will be generated based on source columns and transformations
    -> RTable            -- ^ Source RTable
    -> RTable            -- ^ Target RTable
runTabMapping [] rtabSource     = rtabSource  -- if, there is no transformation left, then just return the source RTable
runTabMapping (rmap:rmaps) rtabSource     = runTabMapping rmaps newRTab  -- else, apply the leftmost mapping and continue with the rest in the list.
    where newRTab = runColMapping rmap rtabSource 
--}


-- | An ETL operation applied to an RTable can be either an ROperation (a relational agebra operation like join, filter etc.) defined in 'RTable' module,
--   or an RColMapping applied to an RTable
data ETLOperation =  
            ETLrOp { rop  :: ROperation   } 
        |   ETLcOp { cmap :: RColMapping } 
                


-- | executes a Unary ETL Operation
etlOpU = runUnaryETLOperation

-- | executes an ETL Operation
runUnaryETLOperation ::
    ETLOperation
    -> RTable  -- ^ input RTable
    -> RTable  -- ^ output RTable
runUnaryETLOperation op inpRtab = 
    case op of 
        ETLrOp { rop  = relOp  } -> ropU relOp inpRtab
        ETLcOp { cmap = colMap } -> runCM colMap inpRtab

-- | executes a Binary ETL Operation
etlOpB = runBinaryETLOperation

-- | executes an ETL Operation
runBinaryETLOperation ::
    ETLOperation
    -> RTable  -- ^ input RTable1
    -> RTable  -- ^ input RTable2    
    -> RTable  -- ^ output RTable
runBinaryETLOperation ETLrOp {rop  = relOp} inpT1 inpT2 = ropB relOp inpT1 inpT2


-- | An ETL operation applied to an RTable can be either an ROperation (a relational agebra operation like join, filter etc.) defined in 'RTable' module,
--   or an RTabMapping applied to an RTable
{--
data ETLOperation =  ROperation | RTabMapping RTable deriving (Eq, Show)
--}

--   Implementation: 
--   An ETL mapping is implemented as a list of ETLOperations that are evaluated from head to tail (left to right). The first ETL operation
--   is evaluated with an input RTable, which is then passed as input to the rest ETL operations.
--   for example an ETLMapping is the following:
--
--   ETLcOP {cmap = map1} : ETLrOp { rop = RFilter} : ETLcOP { cmap = map2 } : ETLrOp { rop = REJoin }
--
-- type ETLMapping = [ETLOperation]




-- | ETLmapping : it is the equivalent of a mapping in an ETL tool and consists of a series of ETLOperations that are applied, one-by-one,
--   to some initial input RTable, but if binary ETLOperations are included in the ETLMapping, then there will be more than one input RTables that
--   the ETLOperations of the ETLMapping will be applied to. When we apply (i.e., run) an ETLOperation of the ETLMapping we get a new RTable,
--   which is then inputed to the next ETLOperation, until we finally run all ETLOperations. The purpose of the execution of an ETLMapping is     
--   to produce a single new RTable as the result of the execution of all the ETLOperations of the ETLMapping.
--   In terms of database operations an ETLMapping is the equivalent of an CREATE AS SELECT (CTAS) operation in an RDBMS. This means that
--   anything that can be done in the SELECT part (i.e., column projection, row filtering, grouping and join operations, etc.)
--   in order to produce a new table, can be included in an ETLMapping.
--
--   An ETLMapping is executed with the etl (runETLmapping) operator
--
--   Implementation: 
--   An ETLMapping is implemented as a binary tree where the node represents the ETLOperation to be executed and the left branch is another 
--   ETLMapping, while the right branch is an RTable (that might be empty in the case of a Unary ETLOperation). 
--   Execution proceeds from bottom-left to top-right.
--   This is similar in concept to a left-deep join tree. In a Left-Deep ETLOperation tree the "pipe" of ETLOperations comes from 
--   the left branches always.
--   The leaf node is always an ETLMapping with an ETLMapEmpty in the left branch and an RTable  in the right branch (the initial RTable inputed
--   to the ETLMapping).
--   In this way, the result of the execution of each ETLOperation (which is an RTable) is passed on to the next ETLOperation. Here is an example:
--
-- @
--     A Left-Deep ETLOperation Tree
-- 
--                              final RTable result
--                                    / 
--                                 etlOp3 
--                              /       \ 
--                           etlOp2     rtab2
--                          /      \ 
-- A leaf-node -->    etlOp1    emptyRTab
--                    /       \
--              ETLMapEmpty   rtab1
--
-- @
--
--  You see that always on the left branch we have an ETLMapping data type (i.e., a left-deep ETLOperation tree). 
--  So how do we implement the following case?
--
-- @
--
--           final RTable result
--             / 
--         etlOp1 
--        /       \
--     rtab1   rtab2
--
-- @
--
-- The answer is that we "model" the left RTable (rtab1 in our example) as an ETLMapping of the form:
--
-- @
--  ETLMapLD { etlOp = ETLcOp{cmap = ColMapEmpty}, tabL = ETLMapEmpty, tabR = rtab1 }
-- @
--
-- So we embed the rtab1 in a ETLMapping, which is a leaf (i.e., it has an empty prevMap), the rtab1 is in 
-- the right branch (tab2) and the ETLOperation is the EmptyColMapping, which returns its input RTable when executed.
-- We can use function 'rtabToETLMapping' for this job.
-- In this manner, a leaf-node can also be implemented like this:
--
-- @
--                              final RTable result
--                                    / 
--                                 etlOp3 
--                              /       \ 
--                           etlOp2     rtab2
--                          /      \ 
-- A leaf-node -->    etlOp1    emptyRTab
--                    /     \
--   rtabToETLMapping rtab1  emptyRTable
-- @
--
data ETLMapping = 
        ETLMapEmpty -- ^ an empty node
    |   ETLMapLD    
        {           etlOp     :: ETLOperation -- ^ the ETLOperation to be executed    
                    ,tabL     :: ETLMapping   -- ^ the left-branch corresponding to the previous ETLOperation, which is input to this one.
                                              --       
                    ,tabR     :: RTable       -- ^ the right branch corresponds to another RTable (for binary ETL operations). 
                                               --  If this is a Unary ETLOperation then this field must be an empty RTable.
        } -- ^ a Left-Deep node
    |   ETLMapRD    
        {           etlOp     :: ETLOperation -- ^ the ETLOperation to be executed    
                    ,tabLrd     :: RTable       -- ^ the left-branch corresponds to another RTable (for binary ETL operations). 
                                              --   If this is a Unary ETLOperation then this field must be an empty RTable.                                                                                             
                    ,tabRrd     :: ETLMapping   -- ^ the right branch corresponding to the previous ETLOperation, which is input to this one.
        } -- ^ a Right-Deep node
    |   ETLMapBal    
        {           etlOp     :: ETLOperation -- ^ the ETLOperation to be executed    
                    ,tabLbal     :: ETLMapping   -- ^ the left-branch corresponding to the previous ETLOperation, which is input to this one. 
                                              --   If this is a Unary ETLOperation then this field might be an empty ETLMapping.                                                                                             
                    ,tabRbal     :: ETLMapping   -- ^ the right branch corresponding corresponding to the previous ETLOperation, which is input to this one.                                               --   If this is a Unary ETLOperation then this field might be an empty ETLMapping.
        } -- ^ a Balanced node


instance Eq ETLMapping where
    etlMap1 == etlMap2 = 
            (etl etlMap1) == (etl etlMap2)  -- two ETLMappings are equal if the RTables resulting from their execution are equal


-- | etl operator executes an ETLmapping
etl = runETLmapping

-- | Executes an ETLMapping
runETLmapping ::
    ETLMapping  -- ^ input ETLMapping
    -> RTable   -- ^ output RTable
-- empty ETL mapping
runETLmapping ETLMapEmpty = emptyRTable
--  ETL mapping with an empty ETLOperation, which is just modelling an RTable
runETLmapping ETLMapLD { etlOp = ETLcOp{cmap = ColMapEmpty}, tabL = ETLMapEmpty, tabR = rtab } = rtab
-- leaf node --> unary ETLOperation on RTable
runETLmapping ETLMapLD { etlOp = runMe, tabL = ETLMapEmpty, tabR = rtab } = if (isRTabEmpty rtab) 
                                                                             then emptyRTable  
                                                                             else etlOpU runMe rtab    
runETLmapping ETLMapLD { etlOp = runMe, tabL = prevmap, tabR = rtab } =
        if (isRTabEmpty rtab)
        then let
                prevRtab = runETLmapping prevmap -- execute previous ETLMapping to get the resulting RTable
             in etlOpU runMe prevRtab
        else let   
                prevRtab = runETLmapping prevmap -- execute previous ETLMapping to get the resulting RTable
             in etlOpB runMe prevRtab rtab

-- | Model an RTable as an ETLMapping which when executed will return the input RTable
rtabToETLMapping ::
       RTable
    -> ETLMapping 
rtabToETLMapping rt =   if (isRTabEmpty rt)
                        then ETLMapEmpty
                        else ETLMapLD { etlOp = ETLcOp {cmap = ColMapEmpty}, tabL = ETLMapEmpty, tabR = rt }

--
--   An ETLMapping is implemented as a series of ETLOperations conected with the :=> operator which is right associative
--  i.e., ETLOp3 :=> ETLOp2 :=> ETLOp1 RTable is  (ETLOp3 :=> (ETLOp2 :=> (ETLOp1 RTable))
--  infixr 9 :=> 
-- data ETLMapping = EmptyETLop RTable | ETLMapping :=> ETLOperation


{--
-- | Executes an ETL mapping 
--   Note htat the source RTables are "embedded" in the data constructors of the ETLMapping data type.
runETLmapping ::
    ETLMapping   -- ^ input ETLMapping
    -> RTable    -- output RTable
runETLmapping EmptyETLop rtab = rtab
runETLmapping etlMap :=> etlOp = runETLmapping etlMap 
    
    case etlOp of
        ETLrOp {rop = relOp} -> runROperation relOp
--}

-- Example of an ETLMapping((<T> TabColTransformation).(<F> RPredicate).(<T> TabTransformation) rtable1) `(<EJ> RJoinPredicate)` rtable2


-- ##################################################
-- *  Various RDataType Transformations
-- ##################################################

-- | rtdappend : Concatenates two Text RDataTypes, in all other cases of RDataType it returns Null.
rdtappend :: 
    RDataType 
    -> RDataType
    -> RDataType
rdtappend (RText t1) (RText t2) = RText (t1 `T.append` t2)
rdtappend _ _ = Null