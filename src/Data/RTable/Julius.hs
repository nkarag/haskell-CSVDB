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

module Data.RTable.Julius (
    evalJulius
    ,ETLMappingExpr (..)
    ,ETLOpExpr (..)
    ,ColMappingExpr (..)
    ,ToColumn (..)
    ,ByFunction (..)
    ,OnRTable (..)
    ,RemoveSrcCol (..)
    ,TabExpr (..)
    ,TabLiteral (..)
    ,ByPred (..)
    ,ROpExpr(..)
    ,RelationalOp (..)
    ,ByGenUnaryOperation (..)
    ,ByGenBinaryOperation (..)
    ,FromRTable (..)
    ,TabExprJoin (..)
    ,Aggregate (..)
    ,AggOp (..)
    ,AsColumn (..)
    ,GroupOnPred (..)
    ) where

-- Data.RTable
import Data.RTable
-- Data.RTable.Etl
import Data.RTable.Etl

-----------------------------------------------------------------
-- Define an Embedded DSL in Haskell' s type system
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
data RemoveSrcCol = RemoveSrc | DontRemoveSrc

-- | <TabExpr> = "Tab" <RTable> | "Previous"
data TabExpr = Tab RTable | Previous
-- | this data type is used for expression where we do not allow the use of the Previous value
data TabLiteral = TabL RTable 

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
--             |    "Join" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "LJoin" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "RJoin" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    "FOJoin" <TabLiteral> <TabExpr> "JoinOn" <RJoinPredicate>
--             |    <TabLiteral> "`Intersect`" <TabExpr> 
--             |    <TabLiteral> "`Union`" <TabExpr> 
--             |    <TabLiteral> "`Minus`" <TabExpr> 
--             |    "GenUnaryOp" "On" <TabExpr> "ByUnaryOp" <UnaryRTableOperation> 
--             |    "GenBinaryOp" <TabLiteral>  <TabExpr>  "ByBinaryOp" <BinaryRTableOperation>
--
data RelationalOp =  
            Filter FromRTable ByPred
        |   Select [ColumnName] FromRTable
        |   Agg Aggregate
        |   GroupBy [ColumnName] Aggregate GroupOnPred
        |   Join TabLiteral TabExpr TabExprJoin
        |   LJoin TabLiteral TabExpr TabExprJoin        
        |   RJoin TabLiteral TabExpr TabExprJoin
        |   FOJoin TabLiteral TabExpr TabExprJoin                
        -- |   TabExpr `Join` TabExprJoin
        -- |   TabExpr `LJoin` TabExprJoin
        -- |   TabExpr `RJoin` TabExprJoin
        |   TabLiteral `Intersect` TabExpr
        |   TabLiteral `Union` TabExpr
        |   TabLiteral `Minus` TabExpr
        |   GenUnaryOp OnRTable ByGenUnaryOperation  -- ^ this is a generic unary operation on a RTable
        |   GenBinaryOp TabLiteral TabExpr ByGenBinaryOperation -- ^ this is a generic binary operation on a RTable

data ByGenUnaryOperation = ByUnaryOp UnaryRTableOperation         
data ByGenBinaryOperation = ByBinaryOp BinaryRTableOperation         

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
            |   Count ColumnName AsColumn    
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
            (EtlR $ (Join (TabL myTable) (Tab myTable2) $ JoinOn myJoinPred) :. ROpEmpty)

-- A 3-way Inner Join => SELECT * FROM myTable JOIN myTable2 ON (myJoinPred) JOIN myTable3 ON (myJoinPred2) JOIN ON myTable4 ON (myJoinPred4)
myEtlExpr9 = --(EtlR $ ( (Tab myTable) `Join` (Tab myTable2 `JoinOn` myJoinPred) ) :. ROpEmpty)
            (EtlR $    (Join (TabL myTable4) Previous $ JoinOn myJoinPred3)
                    :. (Join (TabL myTable3) Previous $ JoinOn myJoinPred2)
                    :. (Join (TabL myTable) (Tab myTable2) $ JoinOn myJoinPred) 
                    :. ROpEmpty
            )            
-- Union => SELECT * FROM myTable UNION SELECT * FROM myTable2
myEtlExpr10 = (EtlR $ (Union (TabL myTable) (Tab myTable2)) :. ROpEmpty)

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


-- | Evaluates (parses) the Julius DSL and produces an ETLMapping
evalJulius :: ETLMappingExpr -> ETLMapping
evalJulius EtlMapEmpty = ETLMapEmpty
evalJulius ((EtlC colMapExpression) :-> restExpression) = 
    case colMapExpression of
        ColMappingEmpty 
            -> connectETLMapLD ETLcOp {cmap = ColMapEmpty} emptyRTable (evalJulius restExpression)  -- (evalJulius restExpression) returns the previous ETLMapping 
        Source srcColumns (Target trgColumns (By colXformation (On tabExpr) removeSource (FilterBy filterPred))) 
            -> let  prevMapping = case tabExpr of
                        Tab tab -> rtabToETLMapping tab -- make the RTable an ETLMapping (this is a leaf node)
                        Previous -> evalJulius restExpression  
                    removeOption = case removeSource of 
                        RemoveSrc -> Yes 
                        DontRemoveSrc -> No
           in connectETLMapLD ETLcOp {cmap = createColMapping srcColumns trgColumns colXformation removeOption filterPred}
                              emptyRTable -- right branch
                              prevMapping -- left branch
evalJulius ((EtlR relOperExpression) :-> restExpression) = 
    let (roperation, leftTabExpr, rightTabExpr) = evalROpExpr relOperExpression
        (prevMapping, rightBranch)  = case (leftTabExpr, rightTabExpr) of
            -- binary operation (leaf)
            (TXE (Tab tab1), TXE (Tab tab2)) -> (rtabToETLMapping tab1, tab2)
            -- binary operation (no leaf)
            (TXE Previous, TXE (Tab tab)) -> (evalJulius restExpression, tab)
            -- unary operation (leaf) - option 1
            (TXE (Tab tab), EmptyTab) -> (rtabToETLMapping tab, emptyRTable)
            -- unary operation (leaf) - option 2
            (EmptyTab, TXE (Tab tab)) -> (ETLMapEmpty, tab)
            -- unary operation (no leaf) 
            (TXE Previous, EmptyTab) -> (evalJulius restExpression, emptyRTable)
            -- everything else is just wrong!! Do nothing
            (_, _) -> (ETLMapEmpty, emptyRTable)
    in connectETLMapLD ETLrOp {rop = roperation} rightBranch prevMapping  -- (evalJulius restExpression) returns the previous ETLMapping 


-- | We use this data type in order to identify unary vs binary operations and if the table is coming from the left or right branch
data TabExprEnhanced = TXE TabExpr | EmptyTab

-- | Evaluates (parses) a Relational Operation Expression of the form 
-- @
--  ROp :. ROp :. ... :. ROpEmpty
-- @
-- and produces the corresponding ROperation data type.
-- This will be an RCombinedOp relational operation that will be the composition of all relational operators
-- in the ROpExpr. In the returned result the TabExpr corresponding to the left and right RTable inputs to the ROperation
-- respectively, are also returned.
evalROpExpr :: ROpExpr -> (ROperation, TabExprEnhanced, TabExprEnhanced)
evalROpExpr ROpEmpty = (ROperationEmpty, EmptyTab, EmptyTab)
evalROpExpr (rop :. restExpression) = 
    case rop of
        Filter (From tabExpr) (FilterBy filterPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = f filterPred -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    

        Select colNameList (From tabExpr) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = p colNameList -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    

        Agg (AggOn aggOpList (From tabExpr)) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = rAgg $ aggOpExprToAggOp aggOpList -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is produced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    
        
        GroupBy colNameList (AggOn aggOpList (From tabExpr)) (GroupOn grpPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = rG grpPred (aggOpExprToAggOp aggOpList) colNameList -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)                    
        
        Join (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = iJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        LJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = lJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        RJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = rJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        FOJoin (TabL tabl) tabExpr (JoinOn joinPred) ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = foJ joinPred tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)
        
        Intersect (TabL tabl) tabExpr ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = i tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        Union (TabL tabl) tabExpr ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = u tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        Minus (TabL tabl) tabExpr ->
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = d tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        -- this is a generic unary operation on a RTable
        GenUnaryOp (On tabExpr) (ByUnaryOp unRTabOp) ->  
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = unRTabOp -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)

        -- this is a generic binary operation on a RTable 
        GenBinaryOp (TabL tabl) tabExpr (ByBinaryOp binRTabOp) -> 
            let -- create current function to be included in a RCombinedOp operation 
                currfunc :: UnaryRTableOperation
                currfunc = binRTabOp tabl -- this returns a RTable -> RTable function
                -- get previous RCombinedOp operation and table expressions
                (prevOperation, prevTXEleft, prevTXEright) = evalROpExpr restExpression                                

            -- the current RCombinedOp is porduced by composing the current function with the previous function
            in case prevOperation of
                    -- in this case the previous operation is a valid non-empty operation and must be composed with the current one
                    RCombinedOp {rcombOp = prevfunc} -> (RCombinedOp {rcombOp = currfunc . prevfunc}, prevTXEleft, EmptyTab)
                    -- in this case the current Operation is the last one (the previous is just empty and must be ignored)
                    ROperationEmpty ->  (RCombinedOp {rcombOp = currfunc}, TXE tabExpr, EmptyTab)


-- | turns the  list of agg operation expressios to a list of RAggOperation data type
aggOpExprToAggOp :: [AggOp] -> [RAggOperation]
aggOpExprToAggOp [] = []
aggOpExprToAggOp (aggopExpr : rest) = 
    let aggop = case aggopExpr of
                    Sum srcCol (As trgCol)      -> (raggSum srcCol trgCol)
                    Count srcCol (As trgCol)    -> (raggCount srcCol trgCol)
                    Min srcCol (As trgCol)      -> (raggMin srcCol trgCol)
                    Max srcCol (As trgCol)      -> (raggMax srcCol trgCol)
                    Avg srcCol (As trgCol)      -> (raggAvg srcCol trgCol)
    in aggop : (aggOpExprToAggOp rest)

            
 
-- and then run the produced ETLMapping
finalRTable = etl $ evalJulius myEtlExpr