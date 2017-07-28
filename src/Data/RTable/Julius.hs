{-|
Module      : Julius
Description : A simple Embedded DSL for ETL-like data processing of RTables
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

module Data.RTable.Julius where

-- Data.RTable
import Data.RTable
-- Data.RTable.Etl
import Data.RTable.Etl

-----------------------------------------------------------------
-- Define a EDSL in Haskell' s type system
-----------------------------------------------------------------

-- | An ETL Mapping Expression. It is a chain of individual ETL Operation Expressions. Each
-- such ETL Operation "acts" on some input RTable and produces a new "transformed" output RTable.
-- an Example:
-- @
--    etl1 :=> etl2 :=> etl3  = 
--    (etl1 :=> (etl2 :=> et3)
-- @
-- So :=> is right associative.
--
-- <ETLMappingExpr> = <ETLOpExpr> { "=>" <ETLOpExpr> } 
--
infixr 5 :->
data ETLMappingExpr = 
            EtlMapEmpty
        |   ETLOpExpr :-> ETLMappingExpr

-- | <ETLOpExpr> = "("EtlC" <ColMappingExpr>")" | "("ETLR" <ROpExpr>")"
data ETLOpExpr = EtlC ColMappingExpr | EtlR ROpExpr

-- | <ColMappingExpr>  =  
--      "Source" "[" <ColName> {,<ColName>} "]" "Target" "[" <ColName> {,<ColName>} "]" ")" "By" <ColTransformation> "On" <TabExpr> ("Yes"|"No") <ByPred>
data ColMappingExpr =  Source [ColumnName] ToColumn | ColMappingEmpty
data ToColumn =  Target [ColumnName] ByFunction
data ByFunction = By ColXForm OnRTable RemoveSrcCol ByPred
--data OnRTable = On RTable  | OnPrevious
data OnRTable = On TabExpr
type ColXForm = [RDataType]  -> [RDataType]
data RemoveSrcCol = RemoveSrc | DontRemoveSrc

-- | <TabExpr> = "Tab" <RTable> | "Previous"
data TabExpr = Tab RTable | Previous

-- | <ByPred> = "FilterBy" <RPredicate>
data ByPred = FilterBy RPredicate

-- | A relational operator can be composed as in Function Composition with the :. operator
-- <ROpExpr> = "("<RelationalOp>")" { ":." <RelationalOp> } 
infixr 6 :.
data ROpExpr =             
            ROpEmpty
        |   RelationalOp :. ROpExpr        

-- | <RelationalOp> = 
--                  "Filter" "From" <TabExpr> <ByPred>
--             |    "Select" "[" <ColName> {,<ColName>} "]  "From" <TabExpr>
--             |    "Agg" <Aggregate>
--             |    "GroupBy" "[" <ColName> {,<ColName>} "]" <Aggregate> "GroupOn" <RGroupPredicate>
--             |    "Join" <TabExpr> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "LJoin" <TabExpr> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "RJoin" <TabExpr> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "FOJoin" <TabExpr> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    <TabExpr> "`Intersect`" <TabExpr> 
--             |    <TabExpr> "`Union`" <TabExpr> 
--             |    <TabExpr> "`Minus`" <TabExpr> 
data RelationalOp =  
            Filter FromRTable ByPred
        |   Select [ColumnName] FromRTable
        |   Agg Aggregate
        |   GroupBy [ColumnName] Aggregate GroupOnPred
        |   Join TabExpr TabExpr TabExprJoin
        |   LJoin TabExpr TabExpr TabExprJoin        
        |   RJoin TabExpr TabExpr TabExprJoin
        |   FOJoin TabExpr TabExpr TabExprJoin                
        -- |   TabExpr `Join` TabExprJoin
        -- |   TabExpr `LJoin` TabExprJoin
        -- |   TabExpr `RJoin` TabExprJoin
        |   TabExpr `Intersect` TabExpr
        |   TabExpr `Union` TabExpr
        |   TabExpr `Minus` TabExpr

-- | <FromRTable> = "From" <TabExpr>
data FromRTable = From TabExpr

-- | <TabExprJoin> = <TabExpr> "`On`" <RJoinPredicate
--data TabExprJoin = TabExpr `JoinOn` RJoinPredicate

-- | <TabExprJoin> = "JoinOn" <RJoinPredicate>
data TabExprJoin = JoinOn RJoinPredicate

-- | <Aggregate> = "AggOn" "[" <AggOp> "]" "From" <TabExpr>
data Aggregate = AggOn [AggOp] FromRTable

-- | AggOp> =   "Sum" <ColName> "As" <ColName> | "Min" <ColName> "As" <ColName> | "Max" <ColName> "As" <ColName> | "Avg" <ColName> "As" <ColName>
data AggOp = 
                Sum ColumnName AsColumn
            |   Min ColumnName AsColumn
            |   Max ColumnName AsColumn
            |   Avg ColumnName AsColumn

-- | <AsColumn> = "As" <ColName>
data AsColumn = As ColumnName            

-- | <GroupOnPred> = "GroupOn" <RGroupPredicate>
data GroupOnPred = GroupOn RGroupPredicate

-----------------------
-- Example:
-----------------------
myTransformation :: ColXForm
myTransformation = undefined

myTransformation2 :: ColXForm
myTransformation2 = undefined

myTable :: RTable
myTable = undefined

myTable2 :: RTable
myTable2 = undefined

myTable3 :: RTable
myTable3 = undefined

myTable4 :: RTable
myTable4 = undefined

myFpred :: RPredicate
myFpred = undefined

myFpred2 :: RPredicate
myFpred2 = undefined

myGpred :: RGroupPredicate
myGpred = undefined

myJoinPred :: RJoinPredicate
myJoinPred = undefined

myJoinPred2 :: RJoinPredicate
myJoinPred2 = undefined

myJoinPred3 :: RJoinPredicate
myJoinPred3 = undefined

-- Empty ETL Mappings
myEtlExpr1 =  EtlMapEmpty
myEtlExpr2 =  EtlC ColMappingEmpty :-> EtlMapEmpty
myEtlExpr3 =  EtlR ROpEmpty :-> EtlMapEmpty

-- Simple Relational Operations examples:
-- Filter => SELECT * FROM myTable WHERE myFpred
myEtlExpr4 = (EtlR $ (Filter (From $ Tab myTable) $ FilterBy myFpred) :. ROpEmpty)
-- Projection  => SELECT col1, col2, col3 FROM myTable
myEtlExpr5 = (EtlR $ (Select ["col1", "col2", "col3"] $ From $ Tab myTable) :. ROpEmpty)
-- Filter then Projection => SELECT col1, col2, col3 FROM myTable WHERE myFpred
myEtlExpr51 = (EtlR $       (Select ["col1", "col2", "col3"] $ From $ Tab myTable)
                        :.  (Filter (From $ Tab myTable) $ FilterBy myFpred) 
                        :.  ROpEmpty
              )
-- Aggregation  => SELECT sum(trgCol2) as trgCol2Sum FROM myTable
myEtlExpr6 = (EtlR $ (Agg $ AggOn [Sum "trgCol2" $ As "trgCol2Sum"] $ From $ Tab myTable) :. ROpEmpty)
-- Group By  => SELECT col1, col2, col3, sum(col4) as col4Sum, max(col4) as col4Max FROM myTable GROUP BY col1, col2, col3
myEtlExpr7 = (EtlR $ (GroupBy  ["col1", "col2", "col3"] (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $ From $ Tab myTable) $ GroupOn myGpred) :. ROpEmpty)
-- Inner Join => SELECT * FROM myTable JOIN myTable2 ON (myJoinPred)
myEtlExpr8 = --(EtlR $ ( (Tab myTable) `Join` (Tab myTable2 `JoinOn` myJoinPred) ) :. ROpEmpty)
            (EtlR $ (Join (Tab myTable) (Tab myTable2) $ JoinOn myJoinPred) :. ROpEmpty)

-- A 3-way Inner Join => SELECT * FROM myTable JOIN myTable2 ON (myJoinPred) JOIN myTable3 ON (myJoinPred2) JOIN ON myTable4 ON (myJoinPred4)
myEtlExpr9 = --(EtlR $ ( (Tab myTable) `Join` (Tab myTable2 `JoinOn` myJoinPred) ) :. ROpEmpty)
            (EtlR $    (Join (Tab myTable4) Previous $ JoinOn myJoinPred3)
                    :. (Join (Tab myTable3) Previous $ JoinOn myJoinPred2)
                    :. (Join (Tab myTable) (Tab myTable2) $ JoinOn myJoinPred) 
                    :. ROpEmpty
            )            
-- Union => SELECT * FROM myTable UNION SELECT * FROM myTable2
myEtlExpr10 = (EtlR $ (Union (Tab myTable) (Tab myTable2)) :. ROpEmpty)

-- A more general example of an ETL mapping including column mappings and relational operations:
-- You read the Julius expression from bottom to top, and results to the follwoing discrete processing steps:      
--      A 1x1 column mapping on myTable based on myTransformation, produces and output table that is input to the next step
--  ->  A filter operation based on myFpred predicate : SELECT * FROM <previous result> WHERE myFpred
--  ->  A group by operation: SELECT col1, col2, col3, SUM(col4) as col2sum,  MAX(col4) as col4Max FROM <previous result>
--  ->  A projection operation: SELECT col1, col2, col3, col4sum FROM <previous result>
--  ->  A 1x1 column mapping on myTable based on myTransformation
--  ->  An aggregation: SELECT SUM(trgCol2) as trgCol2Sum FROM <previous result>
--  ->  A 1xN column mapping based on myTransformation2 
myEtlExpr :: ETLMappingExpr
myEtlExpr =
         (EtlC $ Source ["trgCol2Sum"] $ Target ["newCol1", "newCol2"] $ By myTransformation2 (On Previous) DontRemoveSrc $ FilterBy myFpred2) 
     :-> (EtlR $ (Agg $ AggOn [Sum "trgCol2" $ As "trgCol2Sum"] $ From Previous) :. ROpEmpty )
     :-> (EtlC $ Source ["tgrCol"] $ Target ["trgCol2"] $ By myTransformation (On Previous) RemoveSrc $ FilterBy myFpred2)
     :-> (EtlR $
                -- You read it from bottom to top
             (Select ["col1", "col2", "col3", "col4Sum"] $ From Previous) 
             :. (GroupBy  ["col1", "col2", "col3"] (AggOn [Sum "col4" $ As "col4Sum", Max "col4" $ As "col4Max"] $ From Previous) $ GroupOn myGpred) 
             :. (Filter (From Previous) $ FilterBy myFpred)
             :. ROpEmpty
         )
     :-> (EtlC $ Source ["srcCol"] $ Target ["trgCol"] $ By myTransformation (On $ Tab myTable) DontRemoveSrc $ FilterBy myFpred2)         
     :-> EtlMapEmpty

-- | Evaluates the Julius DSL and producses an ETLMapping
evalJulius :: ETLMappingExpr -> ETLMapping
evalJulius = undefined
 
-- and then run the produced ETLMapping
finalRTable = etl $ evalJulius myEtlExpr