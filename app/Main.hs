{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns #-}  
--{-# LANGUAGE BangPatterns #-}

module Main where

import qualified CSVdb.Base as C
import qualified Data.RTable as T
import qualified Data.RTable.Etl as E
import Data.RTable.Julius

--import RTable
--import QProcessor
--import System.IO
import System.Environment (getArgs)
import Data.Char (toLower)
import Data.HashMap.Strict ((!))
-- Data.Maybe
import Data.Maybe (fromJust)
-- Text 
import Data.Text (Text,stripSuffix, pack, unpack, append,take, takeEnd)
-- Vector
import Data.Vector (toList)
-- List
import Data.List (groupBy)

-- Test command:  stack exec -- csvdb ./misc/test.csv 20 ./misc/testo.csv

main :: IO ()
main = do
    args <- getArgs
    let fi = args!!0
        n = args!!1
        fo = args!!2
    --csv <- C.readCSVFile fi
    csv <- C.readCSV fi
    
    -- debug
    {-- 
    C.writeCSV fo csv
    C.printCSV csv
    --}
    
    --let newcsv = selectNrows (read n) csv
    let rtmdata = T.createRTableMData ( "TestTable", 
                                        [   ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double)
                                        ]
                                      ) 
                                        []  -- primary key
                                        []  -- list of unique keys
        -- *** test CSV to RTable conversion                                        
        rtab = C.csvToRTable rtmdata csv
        rtabNew = T.restrictNrows (read n) rtab
        csvNew = C.rtableToCSV rtmdata rtabNew   

        -- *** test RFilter & RProjection operation
        rtabNew2 =  let
                       myfilter = T.f (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"})
                       myprojection = T.p ["Name","MyTime","Number"]
                       myfilter2 = T.f (\t -> t!"Number" > T.RInt {T.rint = 30 })
                       myprojection2 = T.p ["Name","Number"]
                    in myprojection2.myfilter2.myprojection.myfilter $ rtab     -- ***** THIS IS AN ETL PIPELINE IMPLEMENTED AS FUNCTION COMPOSITION !!!! *****                    
        rtmdata2 = T.createRTableMData ( "TestTable2", 
                                        [   ("Name", T.Varchar),
                                            --("MyDate", T.Date "DD/MM/YYYY"), 
                                           -- ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer) 
                                            --("DNumber", T.Double)
                                        ]
                                      ) 
                                        []  -- primary key
                                        []  -- list of unique keys
        csvNew2 = C.rtableToCSV rtmdata2 rtabNew2  

        -- Test Julius
        rtabNew2_J = E.etl $ evalJulius $
                (EtlR $ 
                       (Select ["Name","Number"] $ From Previous)
                    :. (Filter (From Previous) $ FilterBy (\t -> t!"Number" > T.RInt {T.rint = 30 }))
                    :. (Select ["Name","MyTime","Number"] $ From Previous)
                    :. (Filter (From $ Tab rtab) $ FilterBy (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"}) )
                    :. ROpEmpty)
            :-> EtlMapEmpty

        csvNew2_J = C.rtableToCSV rtmdata2 rtabNew2_J  


    --print / write to file
    C.writeCSV fo csvNew
    C.printCSV csvNew        
    let foName2 = (fromJust (stripSuffix ".csv"  (pack fo))) `mappend` "_t2.csv"
    C.writeCSV (unpack foName2) csvNew2            
    C.printCSV csvNew2
    let foName2_J = (fromJust (stripSuffix ".csv"  (pack fo))) `mappend` "_t2_J.csv"
    C.writeCSV (unpack foName2_J) csvNew2_J
    C.printCSV csvNew2_J


    -- *** test Column Mapping
    -- create a new column holding the doubled value from the source column
    let cmap1 = E.RMap1x1 {E.srcCol = "Number", E.removeSrcCol = E.No, E.trgCol = "NewNumber", E.transform1x1 = \x -> 2*x, E.srcRTupleFilter = \_ -> True}
        rtabNew3 = E.runCM cmap1 rtabNew2
        rtmdata3 = T.createRTableMData ( "TestTable3", 
                                         [   ("Name", T.Varchar)
                                            ,("Number", T.Integer)
                                            ,("NewNumber", T.Integer) 
                                         ]
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    -- Test Julius
        rtabNew3_J = E.etl $ evalJulius $
                    (EtlC $ 
                        Source ["Number"] $
                        Target ["NewNumber"] $
                        By (\[x] -> [2*x]) (On $ Tab rtabNew2)
                        DontRemoveSrc $
                        FilterBy (\_ -> True) )
                :-> EtlMapEmpty

        
    writeResult fo "_t3.csv" rtmdata3 rtabNew3 
    writeResult fo "_t3_J.csv" rtmdata3 rtabNew3_J 

    --     foName3 = (fromJust (stripSuffix ".csv"  (pack fo))) `mappend` "_t3.csv"
    --     csvNew3 = C.rtableToCSV rtmdata3 rtabNew3
    -- C.writeCSV (unpack foName3) csvNew3            
    -- C.printCSV csvNew3

    -- *** test inner join operation
    let rtabNew4 = T.iJ (\t1 t2 -> t1!"Number" == t2!"Number") rtabNew3 rtab 
        rtmdata4 = T.createRTableMData ( "TestTable4", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys
    -- Test Julius
        rtabNew4_J = E.etl $ evalJulius $
                (EtlR $ 
                        (Join (TabL rtabNew3) (Tab rtab) $ JoinOn (\t1 t2 -> t1!"Number" == t2!"Number"))
                    :.  ROpEmpty)
            :-> EtlMapEmpty

        
    writeResult fo "_t4.csv" rtmdata4 rtabNew4
    writeResult fo "_t4_J.csv" rtmdata4 rtabNew4_J

    -- *** Test union, interesection, diff
    let -- change the value in column NewNumber
        cmap2 = E.RMap1x1 {E.srcCol = "NewNumber", E.removeSrcCol = E.No, E.trgCol = "NewNumber", E.transform1x1 = \x -> x + 100, E.srcRTupleFilter = \_ -> True}
        -- now union the two rtables
        rtabNew5 = rtabNew4 `T.u` (E.runCM cmap2 rtabNew4)
        rtmdata5 = T.createRTableMData ( "TestTable5", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

        rtabNew6 = rtabNew5 `T.i` rtabNew4
        rtabNew7 =  rtabNew4 `T.d` rtabNew5

    -- Test Julius
        rtabNew5_J = E.etl $ evalJulius $
                (EtlR $  -- rtabNew5
                        (Union (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlC $ 
                    Source ["NewNumber"] $
                    Target ["NewNumber"] $
                    By (\[x] -> [x + 100]) (On $ Tab rtabNew4)
                    DontRemoveSrc $
                    FilterBy (\_ -> True))                                
            :-> EtlMapEmpty

        rtabNew6_J = E.etl $ evalJulius $
                (EtlR $ -- rtabNew6
                        (Intersect (TabL rtabNew4) (Tab rtabNew5))
                    :.  ROpEmpty)
            :-> EtlMapEmpty

        -- Build rtabNew7 with a single ETL Mapping
        rtabNew7_J = E.etl $ evalJulius $
                (EtlR $
                        (Minus (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlR $ -- rtabNew6
                        (Intersect (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlR $  -- rtabNew5
                        (Union (TabL rtabNew4) (Previous))
                    :.  ROpEmpty)
            :-> (EtlC $ 
                    Source ["NewNumber"] $
                    Target ["NewNumber"] $
                    By (\[x] -> [x + 100]) (On $ Tab rtabNew4)
                    DontRemoveSrc $
                    FilterBy (\_ -> True))                
            :-> EtlMapEmpty

    ---- DEBUG
    -- putStrLn   "------rtabNew3-------" 
    -- print rtabNew3
    -- putStrLn   "------rtab-------" 
    -- print rtab
    -- putStrLn   "------rtabNew4-------"     
    -- print rtabNew4
    -- putStrLn   "------rtmdata5-------" 
    -- print rtmdata5
    -- putStrLn   "------rtabNew5-------" 
    -- print rtabNew5
    ----

    writeResult fo "_t5.csv" rtmdata5 rtabNew5
    writeResult fo "_t5_J.csv" rtmdata5 rtabNew5_J
    writeResult fo "_t6.csv" rtmdata5 rtabNew6
    writeResult fo "_t6_J.csv" rtmdata5 rtabNew6_J
    writeResult fo "_t7.csv" rtmdata5 rtabNew7
    writeResult fo "_t7_J.csv" rtmdata5 rtabNew7_J

    -- *** Change existing column name 
    let -- change the value in column NewNumber
        cmap3 = E.RMap1x1 {E.srcCol = "NewNumber", E.removeSrcCol = E.Yes, E.trgCol = "NewNewNumber", E.transform1x1 = \x -> x, E.srcRTupleFilter = \_ -> True}
        rtabNew8 = E.runCM cmap3 rtabNew5
        rtmdata8 = T.createRTableMData ( "TestTable8", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    writeResult fo "_t8.csv" rtmdata8 rtabNew8                                       

    -- Test a RMapNx1 column mapping
    let 
        cmap4 = E.RMapNx1 {E.srcColGrp = ["Name","MyTime"], E.removeSrcCol = E.Yes, E.trgCol = "New_Nx1_Col", E.transformNx1 = (\[n, T.RTime{T.rtime = t}] -> n `E.rdtappend` (T.RText "<---->") `E.rdtappend` (T.rTimeStampToRText T.stdTimestampFormat t)), E.srcRTupleFilter = \_ -> True}
        rtabNew9 = E.runCM cmap4 rtabNew8
        rtmdata9 = T.createRTableMData ( "TestTable9", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                           -- ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    writeResult fo "_t9.csv" rtmdata9 rtabNew9    

 -- Test a RMap1xN column mapping
    let 
        cmap5 = E.RMap1xN {E.srcCol = "New_Nx1_Col", E.removeSrcCol = E.No, E.trgColGrp = ["1xN_A","1xN_B","1xN_C"], E.transform1xN = (\(T.RText txt) -> [T.RText (Data.Text.take 5 txt), T.RText "<-|-|-|->", T.RText (Data.Text.takeEnd 10 txt)]), E.srcRTupleFilter = \_ -> True}
        rtabNew10 = E.runCM cmap5 rtabNew9
        rtmdata10 = T.createRTableMData ( "TestTable10", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar)
                                            ,("MyDate", T.Date "DD/MM/YYYY") 
                                           -- ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("Number", T.Integer)
                                            ,("DNumber", T.Double)
                                            ,("NewNewNumber", T.Integer)
                                            ,("1xN_A", T.Varchar)
                                            ,("1xN_B", T.Varchar)
                                            ,("1xN_C", T.Varchar)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    writeResult fo "_t10.csv" rtmdata10 rtabNew10                                       

-- Test a RMapNxM column mapping
    let 
        cmap6 = E.RMapNxM {E.srcColGrp = ["1xN_A","1xN_B","1xN_C"], E.removeSrcCol = E.Yes, E.trgColGrp = ["ColNew1","ColNew2"], E.transformNxM = transformation, E.srcRTupleFilter = \_ -> True}
                where
                    transformation [T.RText t1, T.RText t2, T.RText t3] = [T.RText (t1 `append` t3), T.RText t2]
        rtabNew11 = E.runCM cmap6 rtabNew10
        rtmdata11 = T.createRTableMData ( "TestTable11", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar)
                                            ,("MyDate", T.Date "DD/MM/YYYY") 
                                           -- ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("Number", T.Integer)
                                            ,("DNumber", T.Double)
                                            ,("NewNewNumber", T.Integer)
                                            ,("ColNew1", T.Varchar)
                                            ,("ColNew2", T.Varchar)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    writeResult fo "_t11.csv" rtmdata11 rtabNew11                                       

-- Test removeColumn operation
    let 
        rtabNew12 = T.removeColumn "NewNewNumberlalala" (T.removeColumn "MyDate" rtabNew11)        
        rtmdata12 = T.createRTableMData ( "TestTable12", 
                                         [ -- ("Name", T.Varchar),
                                            ("New_Nx1_Col", T.Varchar)
                                      --      ,("MyDate", T.Date "DD/MM/YYYY") 
                                           -- ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("Number", T.Integer)
                                            ,("DNumber", T.Double)
                                      --      ,("NewNewNumber", T.Integer)
                                            ,("ColNew1", T.Varchar)
                                            ,("ColNew2", T.Varchar)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys

    writeResult fo "_t12.csv" rtmdata12 rtabNew12 -- this calls internally C.rtableToCSV and thus it cannot actually test the column removal (because the column removal is hidden
                                                  -- by the RTable metadata). An explicit print of the new RTable is required in order to check correctness.
    print rtabNew12

--  Test combined Roperations
    let
       myfilter = T.f (\t -> t!"Name" == T.RText {T.rtext = "Karagiannidis"})
       myprojection = T.p ["Name","MyTime","Number", "ColNew2"]
       myjoin = T.iJ (\t1 t2 -> t1!"Number" == t2!"Number") rtabNew12  -- !!! Check that the other table participating in the join is embedded in the myjoin operation
       rcombined = myprojection . myjoin . myfilter 
       rtabNew13 = T.rComb rcombined rtab
       rtmdata13 = T.createRTableMData ( "TestTable13", 
                                [   ("Name", T.Varchar)
                                    --,("MyDate", T.Date "DD/MM/YYYY")
                                    ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                    ,("Number", T.Integer) 
                                    --,("DNumber", T.Double)
                                    ,("ColNew2", T.Varchar)
                                ]
                              ) 
                                []  -- primary key
                                []  -- list of unique keys


    writeResult fo "_t13.csv" rtmdata13 rtabNew13
    print rtabNew13

-- Test Left Outer Join
    let rtabNew14 = T.lJ (\t1 t2 -> t1!"Number" == t2!"Number") rtab rtabNew3
        rtmdata14 = T.createRTableMData ( "TestTable14", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys
        
    writeResult fo "_t14.csv" rtmdata14 rtabNew14


-- Test Right Outer Join
    let rtabNew15 = T.rJ (\t1 t2 -> t1!"Number" == t2!"Number") rtabNew3 rtab
        rtmdata15 = T.createRTableMData ( "TestTable15", 
                                         [  ("Name", T.Varchar),
                                            ("MyDate", T.Date "DD/MM/YYYY"), 
                                            ("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("Number", T.Integer), 
                                            ("DNumber", T.Double),
                                            ("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys
        
    writeResult fo "_t15.csv" rtmdata15 rtabNew15


-- Test Aggregation
    -- 
    let rtabNew16 = T.rAgg [T.raggSum "Number" "SumNumber" 
                            , T.raggCount "Number" "CountNumber"
                            , T.raggAvg "Number" "AvgNumber" 
                            , T.raggSum "Name" "SumName"
                            ,T.raggMax "DNumber" "maxDNumber"
                            ,T.raggMax "Number" "maxNumber"
                            ,T.raggMax "Name" "maxName"
                            ,T.raggMin "DNumber" "minDNumber"
                            ,T.raggMin "Number" "minNumber"
                            ,T.raggMin "Name" "minName"
                            ,T.raggAvg "Name" "AvgName"
                         ] rtabNew
        rtmdata16 = T.createRTableMData ( "TestTable16", 
                                         [  --("Name", T.Varchar),
                                            --("MyDate", T.Date "DD/MM/YYYY"), 
                                            --("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS"), 
                                            ("SumNumber", T.Integer) 
                                            ,("CountNumber", T.Integer)
                                            ,("AvgNumber", T.Double)
                                            ,("SumName", T.Integer) 
                                            ,("maxDNumber", T.Double)
                                            ,("maxNumber", T.Integer)
                                            ,("maxName", T.Varchar)
                                            ,("minDNumber", T.Double)
                                            ,("minNumber", T.Integer)
                                            ,("minName", T.Varchar)
                                            ,("AvgName", T.Double)
                                            --,("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys
        
    writeResult fo "_t16.csv" rtmdata16 rtabNew16


-- Test GroupBy
    -- 
    let rtabNew17 = T.rG    (\t1 t2 ->  t1!"Name" == t2!"Name" && t1!"MyTime" == t2!"MyTime")  
                            [   T.raggSum "Number" "SumNumber" 
                                , T.raggCount "Name" "CountName"
                                -- , T.raggAvg "Number" "AvgNumber" 
                                , T.raggSum "Name" "SumName"
                                , T.raggCount "DNumber" "CountDNumber"
                                , T.raggMax "DNumber" "maxDNumber"
                                -- ,T.raggMax "Number" "maxNumber"
                                , T.raggMax "Name" "maxName"
                                -- ,T.raggMin "DNumber" "minDNumber"
                                -- ,T.raggMin "Number" "minNumber"
                                -- ,T.raggMin "Name" "minName"
                                -- ,T.raggAvg "Name" "AvgName"
                             ] 
                             ["Name", "MyTime"]
                             rtabNew

        -- debugging
--        rtupList = toList rtabNew 
--        listOfRTupGroupLists = Data.List.groupBy (\t1 t2 ->  t1!"Name" == t2!"Name") rtupList

        rtmdata17 = T.createRTableMData ( "TestTable17", 
                                         [  ("Name", T.Varchar)
                                            --,("MyDate", T.Date "DD/MM/YYYY"), 
                                            ,("MyTime", T.Timestamp "DD/MM/YYYY HH24:MI:SS")
                                            ,("SumNumber", T.Integer) 
                                            ,("CountName", T.Integer)
                                            -- ,("AvgNumber", T.Double)
                                            ,("SumName", T.Integer) 
                                            ,("CountDNumber", T.Integer)
                                            ,("maxDNumber", T.Double)
                                            -- ,("maxNumber", T.Integer)
                                            ,("maxName", T.Varchar)
                                            -- ,("minDNumber", T.Double)
                                            -- ,("minNumber", T.Integer)
                                            -- ,("minName", T.Varchar)
                                            -- ,("AvgName", T.Double)
                                            --,("NewNumber", T.Integer)
                                         ]                                       
                                       )   
                                       []  -- primary key
                                       []  -- list of unique keys
    
  --  print listOfRTupGroupLists
    writeResult fo "_t17.csv" rtmdata17 rtabNew17


    --print csvNew2
    --return ()
    
    --C.writeCSVFile fo newcsv 
    --C.printCSVFile newcsv

writeResult ::
    String   -- File name
    -> Text  -- new file suffix
    -> T.RTableMData 
    -> T.RTable
    -> IO()
writeResult fname sfx md rt = do
    let foNameNew = (fromJust (stripSuffix ".csv"  (pack fname))) `mappend` sfx
        csvNew = C.rtableToCSV md rt
    C.writeCSV (unpack foNameNew) csvNew            
    C.printCSV csvNew


