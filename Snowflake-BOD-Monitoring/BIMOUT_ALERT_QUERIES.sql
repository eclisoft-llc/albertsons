/*

Use below queries to troubleshoot BimOut data count validation alerts.

If any of the below queries for each kafkaout table returned a result set, then download the result as a csv file and send to the concerned dev engineer (check confluence for contacts) and copy data ops email group.

*/

--ITEM:

SELECT A.DW_CREATE_TS,A.upcId AS SRC_upcId,A.rogCd AS SRC_rogCd,B.* FROM(
                        SELECT    DISTINCT
                    Retail_Order_Group_UPC.UPC_Nbr as upcId,
                    Retail_Order_Group_UPC.Rog_Id as rogCd,
                    Corporate_Item.Corporate_Item_Cd as corporateItemCd,
                    Corporate_Item_UPC_Reference.Prefered_Corporate_Item_Seq_Nbr as preferredSeqNbr,
                    NULL as primaryCicOverride,
                    Corporate_Item_UPC_ROG_Reference.UPC_Type_Cd as upcTypeCd,
                    Corporate_Item_UPC_ROG_Reference.UPC_Type_Dsc as upcTypeDesc,
                    Corporate_Item.Item_Usage_Ind as itemUsageInd,
                    Corporate_Item.Item_Usage_Type_Cd as itemUsageTypeCd,
                    Corporate_Item.Item_Usage_Type_Dsc as itemUsageTypeDsc,
                    Corporate_Item.Display_Item_Ind as displayItemInd,
                    Corporate_Item.Ethnic_Item_Type_Cd as ethnicItmTypCd,
                    Corporate_Item.Special_Packaging_Type_Cd as itmSpcPkgTypCd,
                    Corporate_Item.One_Time_Buy_Ind as oneTmBuyItmSw,
                    Corporate_Item.Organic_Item_Ind as orgncItmSw,
                    Corporate_Item.Seasonal_Item_Type_Cd as seasItmTypCd,
                    Corporate_Item.Item_Pack_UOM_Cd as strRcvPckTypCd, -- mapping not confirmed
                    Corporate_Item.Item_Status_Cd as statusCd,
                    Corporate_Item.Item_Status_Dsc as statusDesc,
                    min_pref_cic.productGroupCd,
                    min_pref_cic.productGroupNm,
                    min_pref_cic.productCategoryCd,
                    min_pref_cic.productCategoryNm,
                    min_pref_cic.productClassCd,
                    min_pref_cic.productClassNm,
                    min_pref_cic.retailSectionCd,
                    min_pref_cic.retailSectionNm,
                    min_pref_cic.productSubClassLevel1Cd,
                    min_pref_cic.productSubClassLevel1Nm,
                    min_pref_cic.productSubClassLevel2Cd,
                    min_pref_cic.productSubClassLevel2Nm,
                    min_pref_cic.UPC_Pack_Ind as packInd,
                    min_pref_cic.UPC_Manufacturer_Id as manufacturerId,
                    min_pref_cic.UPC_Sales_Id as salesId,
                    min_pref_cic.UPC_Country_Id as countryId,
                    min_pref_cic.UPC_System_Id as systemId,
                    min_pref_cic.UPC_Check_Digit as checkDigit,
                    SUPPLY_CHAIN_ITEM.Branch_Item_Cd as branchItemCd,
                    min_pref_cic.Vendor_Product_Id as vendorProductId,
                    Retail_Order_Group_UPC.Manufacturer_Product_Id as manufProductId,
                    min_pref_cic.Shelf_Unit_Size_Dsc as size,
                    min_pref_cic.Shelf_Unit_Size_Qty as sizeQty,
                    min_pref_cic.Shelf_Unit_Size_UOM_Cd as sizeUOMCd,
                    min_pref_cic.Shelf_Unit_Inner_Pack_Qty as innerPack,
                    min_pref_cic.Shelf_Unit_Package_Cd as shelfPackageCd,
                    min_pref_cic.Shelf_Unit_Item_Weight_Cnt as whseItemWgt,
                    --ITEM Dimension Logic requested by ECAT team
                    CASE WHEN Retail_Order_Group_UPC.Retail_Item_Height_Cnt > 0 AND upper(Retail_Order_Group_UPC.Retail_Item_Height_UOM_Cd) = 'FT' THEN (Retail_Order_Group_UPC.Retail_Item_Height_Cnt * 12)
                         WHEN Retail_Order_Group_UPC.Retail_Item_Height_Cnt > 0 AND upper(Retail_Order_Group_UPC.Retail_Item_Height_UOM_Cd) = 'IN' THEN Retail_Order_Group_UPC.Retail_Item_Height_Cnt
                         WHEN Retail_Order_Group_UPC.Retail_Item_Height_Cnt > 0 THEN 1
                         ELSE Retail_Order_Group_UPC.Retail_Item_Height_Cnt
                    END    AS itemHeight,
                    CASE WHEN Retail_Order_Group_UPC.Retail_Item_Depth_Cnt > 0 AND upper(Retail_Order_Group_UPC.Retail_Item_Depth_UOM_Cd) = 'FT' THEN (Retail_Order_Group_UPC.Retail_Item_Depth_Cnt * 12)
                         WHEN Retail_Order_Group_UPC.Retail_Item_Depth_Cnt > 0 AND upper(Retail_Order_Group_UPC.Retail_Item_Depth_UOM_Cd) = 'IN' THEN Retail_Order_Group_UPC.Retail_Item_Depth_Cnt
                         WHEN Retail_Order_Group_UPC.Retail_Item_Depth_Cnt > 0 THEN 1
                         ELSE Retail_Order_Group_UPC.Retail_Item_Depth_Cnt
                    END    AS itemDepth,
                    CASE WHEN Retail_Order_Group_UPC.Retail_Item_Width_Cnt > 0 AND upper(Retail_Order_Group_UPC.Retail_Item_Width_UOM_Cd) = 'FT' THEN (Retail_Order_Group_UPC.Retail_Item_Width_Cnt * 12)
                         WHEN Retail_Order_Group_UPC.Retail_Item_Width_Cnt > 0 AND upper(Retail_Order_Group_UPC.Retail_Item_Width_UOM_Cd) = 'IN' THEN Retail_Order_Group_UPC.Retail_Item_Width_Cnt
                         WHEN Retail_Order_Group_UPC.Retail_Item_Width_Cnt > 0 THEN 1
                         ELSE Retail_Order_Group_UPC.Retail_Item_Width_Cnt
                    END    AS itemWidth,
                    CASE WHEN min_pref_cic.Shelf_Unit_Item_Weight_Cnt > 0 AND upper(min_pref_cic.Shelf_Unit_Item_Weight_UOM_Cd) = 'OZ' THEN (min_pref_cic.Shelf_Unit_Item_Weight_Cnt / 16)
                         WHEN min_pref_cic.Shelf_Unit_Item_Weight_Cnt > 0 AND upper(min_pref_cic.Shelf_Unit_Item_Weight_UOM_Cd) = 'LB' THEN min_pref_cic.Shelf_Unit_Item_Weight_Cnt
                         WHEN min_pref_cic.Shelf_Unit_Item_Weight_Cnt > 0 THEN 1
                         ELSE min_pref_cic.Shelf_Unit_Item_Weight_Cnt
                    END    AS itemWeight,
                    --Retail_Order_Group_UPC.Retail_Item_Pack_UOM_Cd as unitOfMeasure,
                    --Retail_Order_Group_UPC.Shelf_Unit_Item_Height_Cnt as itemHeight,
                    --Retail_Order_Group_UPC.Shelf_Unit_Item_Depth_Cnt as itemDepth,
                    --Retail_Order_Group_UPC.Shelf_Unit_Item_Width_Cnt as itemWidth,
                    --Retail_Order_Group_UPC.Shelf_Unit_Item_Weight_Cnt as itemWeight,
                    --Retail_Order_Group_UPC.Shelf_Unit_Item_Pack_UOM_Cd as unitOfMeasure,
                    min_pref_cic.Shelf_Unit_Size_UOM_Cd as unitOfMeasure,
                    Retail_Order_Group_UPC.Retail_Unit_Pack_Nbr as retailUnitPackNbr ,
                    min_pref_cic.retailItemDsc,
                    min_pref_cic.internetItemDsc,
                    min_pref_cic.warehouseItemDsc,
                    Retail_Order_Group_UPC.DSD_Ind as dsdInd,
                    Retail_Order_Group_UPC.Warehouse_Ind as warehouseInd,
                    CASE WHEN min_pref_cic.MANUFACTURER_TYPE_IND = 'H' THEN 'Y' ELSE 'N' END as manufacturingPlantInd,
                    Retail_Order_Group_UPC.Price_Lookup_Cd as priceLookUpCd,
                    Retail_Order_Group_UPC.Food_Stamp_Cd as foodStampCd,
                    Retail_Order_Group_UPC.Department_Override_Cd as deptOverrideCd,
                    min_pref_cic.brandCd, 
                    min_pref_cic.brandNm, 
                    Retail_Order_Group_UPC.Unit_Price_Measure_Nbr as unitPriceMeasureNbr,
                    Retail_Order_Group_UPC.Unit_Price_Label_Unit_Cd as unitPriceLabelUnit,
                    Retail_Order_Group_UPC.Unit_Price_Multiplication_Fctr as unitPriceMultiFactor,
                    Retail_Order_Group_UPC.Unit_Price_Measure_Cd as unitPriceMeasureCd,
                    Retail_Order_Group_UPC.Unit_Price_Measure_Unit as unitPriceMeasureUnit, -- mapping not yet confirmed
                    Retail_Order_Group_UPC.Unit_Price_Table_Nbr as unitPriceTableCd,
                    NULL as sellByWeightInd, -- mapping listed as N/A
                    Retail_Order_Group_UPC.Ring_Type_Cd as ringTypeCd,
                    NULL as displayCd, -- mapping listed as N/A
                    cw.Warning_Received_Ts as warnRecvTs,
                    cw.Effective_Start_Ts as crtTimeStamp,
                    cw.Vendor_Id as vendNbr,
                    cw.Food_Ind as foodIndicator,
                    cw.Message_Long_Txt as warnMsgTxt
                    ,NVL(Retail_Order_Group_UPC.DW_SOURCE_UPDATE_NM,Retail_Order_Group_UPC.DW_SOURCE_CREATE_NM) AS DW_SOURCE_CREATE_NM
                    ,CURRENT_TIMESTAMP as DW_CREATE_TS 
                    ,false as DW_LOGICAL_DELETE_IND
                    ,CASE     WHEN Corporate_Item_UPC_Reference.Prefered_Corporate_Item_Seq_Nbr is NULL THEN 'preferredSeqNbr is NULL'
                            WHEN min_pref_cic.productGroupCd is NULL THEN 'productGroupCd is NULL'
                            WHEN min_pref_cic.productCategoryCd is NULL THEN 'productCategoryCd is NULL'
                            WHEN min_pref_cic.productClassCd is NULL THEN 'productClassCd is NULL'
                            WHEN min_pref_cic.productSubClassLevel1Cd is NULL THEN 'productSubClassLevel1Cd is NULL'
                            WHEN min_pref_cic.productSubClassLevel2Cd is NULL THEN 'productSubClassLevel2Cd is NULL'
                            WHEN min_pref_cic.internetItemDsc is NULL or trim(min_pref_cic.internetItemDsc) = '' THEN 'internetItemDsc is Blank' 
                            WHEN min_pref_cic.UPC_Pack_Ind is NULL THEN 'UPC Component: packInd is NULL'
                            WHEN min_pref_cic.UPC_Manufacturer_Id is NULL THEN 'UPC Component: manufacturerId is NULL'
                            WHEN min_pref_cic.UPC_Sales_Id is NULL THEN 'UPC Component: salesId is NULL'
                            WHEN min_pref_cic.UPC_Country_Id is NULL THEN 'UPC Component: countryId is NULL' 
                            WHEN min_pref_cic.UPC_System_Id is NULL THEN 'UPC Component: systemId is NULL'
                            WHEN min_pref_cic.UPC_Check_Digit is NULL THEN 'UPC Component: checkDigit is NULL'
                    --        WHEN Retail_Order_Group_UPC.Branch_Item_Cd is NULL THEN 'UPC Component: branchItemCd is NULL'
                            ELSE NULL
                    END    AS EXCEP_FLAG,
                    Retail_Order_Group_UPC.TRADING_STAMP_CD as tradingStamp
            FROM    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Retail_Order_Group_UPC Retail_Order_Group_UPC
            JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item_UPC_ROG_Reference    ON    Corporate_Item_UPC_ROG_Reference.UPC_Nbr = Retail_Order_Group_UPC.UPC_Nbr
                                                                                            AND    Corporate_Item_UPC_ROG_Reference.Rog_Id    = Retail_Order_Group_UPC.Rog_Id
                                                                                            AND    Corporate_Item_UPC_ROG_Reference.DW_CURRENT_VERSION_IND = TRUE
                                                                                            AND    Corporate_Item_UPC_ROG_Reference.DW_LOGICAL_DELETE_IND = FALSE
            JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item_UPC_ROG_Retail    ON    Corporate_Item_UPC_ROG_Reference.UPC_Nbr = Corporate_Item_UPC_ROG_Retail.UPC_Nbr
                                                                                            AND    Corporate_Item_UPC_ROG_Reference.Rog_Id    = Corporate_Item_UPC_ROG_Retail.Rog_Id
                                                                                            AND    Corporate_Item_UPC_ROG_Reference.Corporate_Item_Integration_Id    = Corporate_Item_UPC_ROG_Retail.Corporate_Item_Integration_Id
                                                                                            AND    Corporate_Item_UPC_ROG_Retail.DW_CURRENT_VERSION_IND = TRUE
                                                                                            AND    Corporate_Item_UPC_ROG_Retail.DW_LOGICAL_DELETE_IND = FALSE
            JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item         ON     Corporate_Item.Corporate_Item_Integration_Id = Corporate_Item_UPC_ROG_Reference.Corporate_Item_Integration_Id
                                                                                AND Corporate_Item.DW_CURRENT_VERSION_IND = TRUE
                                                                                AND    Corporate_Item.DW_LOGICAL_DELETE_IND = FALSE
            JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item_UPC_Reference    ON    Corporate_Item_UPC_Reference.Corporate_Item_Integration_Id = Corporate_Item_UPC_ROG_Reference.Corporate_Item_Integration_Id
                                                                                        AND    Corporate_Item_UPC_Reference.UPC_Nbr = Corporate_Item_UPC_ROG_Reference.UPC_Nbr
                                                                                        AND    Corporate_Item_UPC_Reference.DW_CURRENT_VERSION_IND = TRUE
                                                                                        AND    Corporate_Item_UPC_Reference.DW_LOGICAL_DELETE_IND = FALSE
            JOIN    (
                        SELECT    rog_upc_minpref_cic.ROG_ID,
                                rog_upc_minpref_cic.UPC_Nbr,
                                rog_upc_minpref_cic.Corporate_Item_Integration_Id,
                                Corporate_Item.SMIC_Group_Cd as productGroupCd,
                                SMIC_Group.SMIC_Group_Nm as productGroupNm,
                                Corporate_Item.SMIC_Category_Cd as productCategoryCd,
                                SMIC_Category.SMIC_Category_Nm as productCategoryNm,
                                Corporate_Item.SMIC_Class_Cd as productClassCd,
                                SMIC_Class.SMIC_Class_Nm as productClassNm,
                                rog_upc_minpref_cic.UPC_Pack_Ind,
                                rog_upc_minpref_cic.UPC_Manufacturer_Id,
                                rog_upc_minpref_cic.UPC_Sales_Id,
                                rog_upc_minpref_cic.UPC_Country_Id,
                                rog_upc_minpref_cic.UPC_System_Id,
                                rog_upc_minpref_cic.UPC_Check_Digit,
                                rog_upc_minpref_cic.Retail_Section_Cd as retailSectionCd,
                                Section.Section_Nm as retailSectionNm,
                                Corporate_Item.SMIC_Sub_Class_Cd as productSubClassLevel1Cd,
                                SMIC_Sub_Class.SMIC_Sub_Class_Nm as productSubClassLevel1Nm,
                                Corporate_Item.SMIC_Sub_Sub_Class_Cd as productSubClassLevel2Cd,
                                SMIC_Sub_Sub_Class.SMIC_Sub_Sub_Class_Nm as productSubClassLevel2Nm,
                                Corporate_Item.Retail_Item_Dsc as retailItemDsc,
                                Corporate_Item.Internet_Item_Dsc as internetItemDsc,
                                Corporate_Item.Warehouse_Item_Dsc as warehouseItemDsc,
                                Corporate_Item.Brand_Cd as brandCd,
                                Corporate_Item.Brand_Nm as brandNm,
                                Corporate_Item.Vendor_Product_Id,
                                Corporate_Item.Shelf_Unit_Size_Dsc,
                                Corporate_Item.Size_Qty as Shelf_Unit_Size_Qty,
                                Corporate_Item.Size_UOM_Cd as Shelf_Unit_Size_UOM_Cd,
                                Corporate_Item.Inner_Pack_Qty as Shelf_Unit_Inner_Pack_Qty,
                                Corporate_Item.Shelf_Package_Cd as Shelf_Unit_Package_Cd,
                                Corporate_Item.Item_Weight_Cnt as Shelf_Unit_Item_Weight_Cnt,
                                Corporate_Item.Item_Weight_UOM_Cd as Shelf_Unit_Item_Weight_UOM_Cd,
                                Corporate_Item.MANUFACTURER_TYPE_IND as MANUFACTURER_TYPE_IND
                        FROM    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item        
                        JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.SMIC_Group            ON    SMIC_Group.SMIC_Group_Cd                     = Corporate_Item.SMIC_Group_Cd    
                                                                                            AND SMIC_Group.DW_CURRENT_VERSION_IND = TRUE
                        JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.SMIC_Category        ON    SMIC_Category.SMIC_Group_Cd                    = Corporate_Item.SMIC_Group_Cd    
                                                                                            AND    SMIC_Category.SMIC_Category_Cd                 = Corporate_Item.SMIC_Category_Cd 
                                                                                            AND SMIC_Category.DW_CURRENT_VERSION_IND = TRUE
                        JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.SMIC_Class            ON    SMIC_Class.SMIC_Group_Cd                    = Corporate_Item.SMIC_Group_Cd    
                                                                                            AND    SMIC_Class.SMIC_Category_Cd                 = Corporate_Item.SMIC_Category_Cd
                                                                                            AND SMIC_Class.SMIC_Class_Cd                     = Corporate_Item.SMIC_Class_Cd 
                                                                                            AND SMIC_Class.DW_CURRENT_VERSION_IND = TRUE
                        JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.SMIC_Sub_Class        ON    SMIC_Sub_Class.SMIC_Group_Cd                = Corporate_Item.SMIC_Group_Cd    
                                                                                            AND    SMIC_Sub_Class.SMIC_Category_Cd             = Corporate_Item.SMIC_Category_Cd
                                                                                            AND SMIC_Sub_Class.SMIC_Class_Cd                 = Corporate_Item.SMIC_Class_Cd
                                                                                            AND    SMIC_Sub_Class.SMIC_Sub_Class_Cd             = Corporate_Item.SMIC_Sub_Class_Cd 
                                                                                            AND SMIC_Sub_Class.DW_CURRENT_VERSION_IND = TRUE
                        JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.SMIC_Sub_Sub_Class    ON    SMIC_Sub_Sub_Class.SMIC_Group_Cd            = Corporate_Item.SMIC_Group_Cd    
                                                                                            AND    SMIC_Sub_Sub_Class.SMIC_Category_Cd         = Corporate_Item.SMIC_Category_Cd
                                                                                            AND SMIC_Sub_Sub_Class.SMIC_Class_Cd             = Corporate_Item.SMIC_Class_Cd
                                                                                            AND    SMIC_Sub_Sub_Class.SMIC_Sub_Class_Cd         = Corporate_Item.SMIC_Sub_Class_Cd 
                                                                                            AND SMIC_Sub_Sub_Class.SMIC_Sub_Sub_Class_Cd     = Corporate_Item.SMIC_Sub_Sub_Class_Cd 
                                                                                            AND SMIC_Sub_Sub_Class.DW_CURRENT_VERSION_IND = TRUE
                        JOIN (
                                    SELECT *
                                    FROM 
                                        (
                                            SELECT    ROG_ID
                                                    ,Corporate_Item_UPC_ROG_Reference.UPC_NBR
                                                    ,Corporate_Item_UPC_ROG_Reference.Corporate_Item_Integration_Id
                                                    ,Corporate_Item_UPC_ROG_Reference.RETAIL_SECTION_CD
                                                    ,Corporate_Item_UPC_ROG_Reference.UPC_Pack_Ind 
                                                    ,Corporate_Item_UPC_ROG_Reference.UPC_Manufacturer_Id
                                                    ,Corporate_Item_UPC_ROG_Reference.UPC_Sales_Id
                                                    ,Corporate_Item_UPC_ROG_Reference.UPC_Country_Id
                                                    ,Corporate_Item_UPC_ROG_Reference.UPC_System_Id
                                                    ,Corporate_Item_UPC_ROG_Reference.UPC_Check_Digit
                                                    ,row_number() over(partition by ROG_ID, Corporate_Item_UPC_ROG_Reference.UPC_NBR ORDER BY Prefered_Corporate_Item_Seq_Nbr asc) as rn 
                                            FROM    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item_UPC_ROG_Reference
                                            JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item_UPC_Reference
                                            ON        Corporate_Item_UPC_ROG_Reference.UPC_NBR = Corporate_Item_UPC_Reference.UPC_NBR
                                            AND        Corporate_Item_UPC_ROG_Reference.Corporate_Item_Integration_Id = Corporate_Item_UPC_Reference.Corporate_Item_Integration_Id
                                            AND        Corporate_Item_UPC_Reference.DW_CURRENT_VERSION_IND = TRUE
                                            AND     Corporate_Item_UPC_Reference.DW_LOGICAL_DELETE_IND = FALSE
                                            JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Corporate_Item     
                                            ON         Corporate_Item.Corporate_Item_Integration_Id = Corporate_Item_UPC_ROG_Reference.Corporate_Item_Integration_Id
                                            AND     Corporate_Item.DW_CURRENT_VERSION_IND = TRUE
                                            AND        Corporate_Item.DW_LOGICAL_DELETE_IND = FALSE
                                            WHERE        Corporate_Item_UPC_ROG_Reference.DW_CURRENT_VERSION_IND = TRUE
                                            AND            Corporate_Item_UPC_ROG_Reference.DW_LOGICAL_DELETE_IND = FALSE
                                            AND            Corporate_Item.Item_Status_Cd = 'V'
                                            AND            Corporate_Item.Item_Usage_Ind = 'R'
                                            AND            Corporate_Item.Item_Usage_Type_Cd in ('R','S')
                                            AND            NVL(Corporate_Item.Display_Item_Ind,'-1') <> 'Y'
                                            AND            Corporate_Item_UPC_ROG_Reference.Retail_Status_Ind = 'V'
                                            AND            Corporate_Item_UPC_ROG_Reference.Status_Cd = 'V'
                                        )
                                    WHERE rn = 1
                                ) rog_upc_minpref_cic    ON     Corporate_Item.Corporate_Item_Integration_Id = rog_upc_minpref_cic.Corporate_Item_Integration_Id
                        LEFT JOIN    (    SELECT Section_Cd, Section_Nm 
                                        FROM EDM_CONFIRMED_PRD.DW_C_LOCATION.SECTION
                                        WHERE DW_CURRENT_VERSION_IND = TRUE
                                    ) Section                                                 ON    Section.Section_Cd    = rog_upc_minpref_cic.RETAIL_SECTION_CD
                        WHERE    Corporate_Item.DW_CURRENT_VERSION_IND = TRUE
                        AND        Corporate_Item.DW_LOGICAL_DELETE_IND = FALSE
                    ) min_pref_cic ON Retail_Order_Group_UPC.ROG_ID = min_pref_cic.ROG_ID AND Retail_Order_Group_UPC.UPC_Nbr = min_pref_cic.UPC_Nbr
            LEFT JOIN
            (
                SELECT *
                FROM (
                        SELECT    Consumer_Warning.UPC_Nbr,
                                Consumer_Warning_Source.Warning_Received_Ts,
                                Consumer_Warning_Source.Effective_Start_Ts,
                                Consumer_Warning.Vendor_Id,
                                Consumer_Warning.Food_Ind,
                                Consumer_Warning_Message.Message_Long_Txt,
                                row_number() over (partition by Consumer_Warning.UPC_Nbr order by Consumer_Warning_Source.Warning_Received_Ts desc) as rn
                        FROM    (SELECT DISTINCT * FROM EDM_CONFIRMED_PRD.DW_C_PRODUCT.Consumer_Warning WHERE DW_CURRENT_VERSION_IND = TRUE) Consumer_Warning     //---- chance of duplicates since not all keys were joined according to Consumer Warning data model. But the incoming Consumer Warning data might not produce duplicates.
                        LEFT JOIN    (SELECT DISTINCT * FROM EDM_CONFIRMED_PRD.DW_C_PRODUCT.Consumer_Warning_Source) Consumer_Warning_Source     ON    Consumer_Warning_Source.Consumer_Warning_Integration_Id = Consumer_Warning.Consumer_Warning_Integration_Id
                                                                                                     -- chance of duplicates since not all keys were joined according to Consumer Warning data model. But the incoming data might not produce duplicates.
                        LEFT JOIN    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Consumer_Warning_Message     ON    Consumer_Warning_Message.Consumer_Warning_Integration_Id = Consumer_Warning_Source.Consumer_Warning_Integration_Id
                                                                                                        AND Consumer_Warning_Message.Warning_Source_Type_Cd = Consumer_Warning_Source.Warning_Source_Type_Cd
                                                                                                        AND    Consumer_Warning_Message.Warning_Source_Nm = Consumer_Warning_Source.Warning_Source_Nm
                                                                                                        AND    Consumer_Warning_Message.Warning_Received_Ts = Consumer_Warning_Source.Warning_Received_Ts
                        WHERE     upper(Consumer_Warning_Source.Warning_Source_Nm) = 'PROP65'
                    )
                WHERE RN = 1
            ) cw     ON    CW.UPC_Nbr = Retail_Order_Group_UPC.UPC_Nbr
            JOIN        EDM_CONFIRMED_PRD.DW_C_PRODUCT.Supply_Chain_Item SCI     ON    SCI.Warehouse_id = Corporate_Item_UPC_ROG_Retail.Item_Sourcing_facility_id
                                                                                    AND    SCI.Corporate_Item_Integration_Id = Corporate_Item_UPC_ROG_Retail.Corporate_Item_Integration_Id
                                                                                    AND    SCI.DW_CURRENT_VERSION_IND = TRUE
                                                                                    AND    SCI.DW_LOGICAL_DELETE_IND = FALSE
            LEFT JOIN    (   SELECT  Warehouse_id, 
                                    Corporate_Item_Integration_Id, 
                                    branch_item_cd 
                            FROM    EDM_CONFIRMED_PRD.DW_C_PRODUCT.Supply_Chain_Item
                            WHERE   DW_CURRENT_VERSION_IND = TRUE
                            AND     DW_LOGICAL_DELETE_IND = FALSE
                            --GROUP BY Division_id, 
                            --        Corporate_Item_Integration_Id
                         ) Supply_Chain_Item       ON    Supply_Chain_Item.warehouse_id = Corporate_Item_UPC_ROG_Retail.Item_Sourcing_facility_id
                                                AND    Supply_Chain_Item.Corporate_Item_Integration_Id = min_pref_cic.Corporate_Item_Integration_Id
            WHERE        Retail_Order_Group_UPC.DW_CURRENT_VERSION_IND = TRUE
            AND            Retail_Order_Group_UPC.DW_LOGICAL_DELETE_IND = FALSE
            AND            Corporate_Item.Item_Status_Cd = 'V'
            AND            Corporate_Item.Item_Usage_Ind = 'R'
            AND            Corporate_Item.Item_Usage_Type_Cd in ('R','S')
            AND            NVL(Corporate_Item.Display_Item_Ind,'-1') <> 'Y'
            AND            Corporate_Item_UPC_ROG_Reference.Retail_Status_Ind = 'V'
            AND            Corporate_Item_UPC_ROG_Reference.Status_Cd = 'V'
            AND            NVL(Retail_Order_Group_UPC.Price_Required_Ind,'-1') <> 'Y'
            AND            SCI.Warehouse_Item_Status_Cd = 'V'
            AND            Retail_Order_Group_UPC.Rog_Id in (SELECT DISTINCT ROGCD FROM EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_ROG_CONTROL WHERE INCREMENTLOADFLAG = TRUE)) AS A

FULL OUTER JOIN (select DISTINCT upcId, rogCd
from ECATALOG_ITEM
WHERE DW_LOGICAL_DELETE_IND = FALSE) AS B
ON B.UPCID=A.UPCID AND B.ROGCD=A.rogCd --AND A.Corporate_Item_Cd =B.corporateItemCd 
WHERE B.UPCID IS NULL OR A.UPCID IS NULL


--ROGPRICE

                               SELECT     
                                    retailsectioncd,
                                    rogCd,
                                    upcId,
                                    divisionId,
                                    priceAreaCd,
                                    upTblNum,
                                    upMeasure,
                                    upMeasureUnit,
                                    upLabelUnit,
                                    upMultFctr,
                                    itemPriceMethodCd,
                                    itemPriceAmt,
                                    itemPriceLimtQty,
                                    itemPriceFactor,
                                    itemPriceAlternatePriceAmt,
                                    itemPriceAlternatePriceFactor,
                                    regularRetailItemPriceAmt,
                                    regularRetailItemPriceFactor,
                                    regularRetailItemAltPriceAmt,
                                    regularRetailItemAltPriceFactor,
                                    regularItemLimitQty,
                                    regularPriceMethodCd,
                                    reglarPriceChangeDt,
                                    pendingPricePriceMethodCd,
                                    pendingPriceItemLimtQty,
                                    pendingPriceItemPriceAmt,
                                    pendingPriceItemPriceFactor,
                                    pendingPriceAlternatePriceAmt,
                                    pendingPriceAlternatePriceFactor,
                                    pendingPriceEffectiveStartDt,
                                    pendingPriceEffectiveEndDt,
                                    pendingPriceLongTermSpecialLnd,
                                    deleteYN,
                                    DW_SOURCE_CREATE_NM,
                                    createtime,
                                    modifiedtime FROM (                                    select distinct
                                             src.retailsectioncd,
                                            CAST(src.rogCd AS VARCHAR(16777216)) AS rogCd,
                                            CAST(src.upcId AS FLOAT) AS upcId,
                                            CAST(src.divisionId AS VARCHAR(16777216)) AS divisionId,
                                            CAST(src.priceAreaCd AS VARCHAR(16777216)) AS priceAreaCd,
                                            CAST(src.upTblNum AS FLOAT) AS upTblNum,
                                            CAST(src.upMeasure AS FLOAT) AS upMeasure,       
                                            CAST(src.upMeasureUnit AS VARCHAR(16777216)) AS upMeasureUnit,
                                            CAST(src.upLabelUnit AS VARCHAR(16777216)) AS upLabelUnit,
                                            CAST(src.upMultFctr AS FLOAT) AS upMultFctr,
                                            CAST(src.itemPriceMethodCd AS VARCHAR(16777216)) AS itemPriceMethodCd,                                           
                                            CAST(src.itemPriceAmt AS FLOAT) AS itemPriceAmt,
                                            CAST(src.itemPriceLimtQty AS FLOAT) AS itemPriceLimtQty,
                                            CAST(src.itemPriceFactor AS FLOAT) AS itemPriceFactor,
                                            CAST(src.itemPriceAlternatePriceAmt AS FLOAT) AS itemPriceAlternatePriceAmt,
                                            CAST(src.itemPriceAlternatePriceFactor AS FLOAT) AS itemPriceAlternatePriceFactor,
                                            CAST(src.regularRetailItemPriceAmt AS FLOAT) AS regularRetailItemPriceAmt,
                                            CAST(src.regularRetailItemPriceFactor AS FLOAT) AS regularRetailItemPriceFactor,
                                            CAST(src.regularRetailItemAltPriceAmt AS FLOAT) AS regularRetailItemAltPriceAmt,
                                            CAST(src.regularRetailItemAltPriceFactor AS FLOAT) AS regularRetailItemAltPriceFactor,
                                            CAST(src.regularItemLimitQty AS FLOAT) AS regularItemLimitQty,
                                            CAST(src.regularPriceMethodCd AS VARCHAR(16777216)) AS regularPriceMethodCd,
                                            CAST(src.reglarPriceChangeDt AS DATE) AS reglarPriceChangeDt,
                                            CAST(src.pendingPricePriceMethodCd AS VARCHAR(16777216)) AS pendingPricePriceMethodCd,
                                            CAST(src.pendingPriceItemLimtQty AS FLOAT) AS pendingPriceItemLimtQty,
                                            CAST(src.pendingPriceItemPriceAmt AS FLOAT) AS pendingPriceItemPriceAmt,
                                            CAST(src.pendingPriceItemPriceFactor AS FLOAT) AS pendingPriceItemPriceFactor,
                                            CAST(src.pendingPriceAlternatePriceAmt AS FLOAT) AS pendingPriceAlternatePriceAmt,
                                            CAST(src.pendingPriceAlternatePriceFactor AS FLOAT) AS pendingPriceAlternatePriceFactor,
                                            CAST(src.pendingPriceEffectiveStartDt AS DATE) AS pendingPriceEffectiveStartDt,
                                            CAST(src.pendingPriceEffectiveEndDt AS DATE) AS pendingPriceEffectiveEndDt,
                                            CAST(src.pendingPriceLongTermSpecialLnd AS VARCHAR(16777216)) AS pendingPriceLongTermSpecialLnd,
                                            src.deleteYN,
                                            src.DW_SOURCE_CREATE_NM,
                                            src.createtime,
                                            src.modifiedtime
                                     from (
                                    select 
                                        CIURR.retail_section_cd as retailsectioncd,
                                       cp.rog_id  as rogCd,                                    
                                       cp.upc_nbr as upcId,
                                       rogdiv.DIVISION_ID as divisionid, 
                                       cp.Price_Area_Cd  as priceAreaCd,
                                       rogupc.UNIT_PRICE_TABLE_NBR as upTblNum,
                                       rogupc.unit_Price_Measure_Nbr  as  upMeasure, 
                                       rogupc.Unit_Price_Measure_Unit as upMeasureUnit,
                                       rogupc.UNIT_PRICE_LABEL_UNIT_CD as upLabelUnit,
                                        rogupc.UNIT_PRICE_MULTIPLICATION_FCTR as upMultFctr,
                                        cp.Price_Method_Cd as itemPriceMethodCd,
                                        cp.Item_Price_Amt as itemPriceAmt,
                                        cp.Item_Limt_Qty as itemPriceLimtQty,
                                        cp.Item_Price_Fctr as itemPriceFactor,
                                        cp.Alternate_Price_Amt  as itemPriceAlternatePriceAmt,
                                        cp.Alternate_Price_Fctr as itemPriceAlternatePriceFactor,
                                        cp.Regular_Retail_Item_Price_Amt as regularRetailItemPriceAmt,
                                        cp.Regular_Retail_Item_Price_Fctr as regularRetailItemPriceFactor,
                                        cp.Regular_Retail_Item_Alternate_Price_Amt as regularRetailItemAltPriceAmt,
                                        cp.Regular_Retail_Item_Alternate_Price_Fctr as regularRetailItemAltPriceFactor,
                                        cp.Regular_Item_Limit_Qty as regularItemLimitQty,
                                        cp.Regular_Price_Method_Cd as regularPriceMethodCd,
                                        cp.Current_Price_Change_Dt as reglarPriceChangeDt,
                                        pp.Price_Method_Cd as pendingPricePriceMethodCd,
                                        pp.Item_Limt_Qty as pendingPriceItemLimtQty,
                                        pp.Item_Price_Amt as pendingPriceItemPriceAmt,
                                        pp.Item_Price_Fctr as pendingPriceItemPriceFactor,
                                        pp.Alternate_Price_Amt as pendingPriceAlternatePriceAmt,
                                        pp.Alternate_Price_Fctr as pendingPriceAlternatePriceFactor,
                                        pp.Price_Effective_Start_Dt as pendingPriceEffectiveStartDt,
                                        pp.Price_Effective_End_Dt as pendingPriceEffectiveEndDt,
                                        pp.Long_Term_Special_Ind as pendingPriceLongTermSpecialLnd,
                                        cp.DW_LOGICAL_DELETE_IND as deleteYN,
                                        cp.DW_SOURCE_CREATE_NM,
                                        rpc.createtime, 
                                        rpc.modifiedtime
                                     from 
                                     EDM_CONFIRMED_PRD.DW_C_PRODUCT.Current_Retail_Item_Price cp
                                     left outer join
                                                (select distinct corporate_item_integration_id,upc_nbr, ROG_ID,Retail_Section_Cd
                                                    from EDM_CONFIRMED_PRD.DW_C_PRODUCT.CORPORATE_ITEM_UPC_ROG_REFERENCE
                                                    where DW_Last_Effective_Dt = '9999-12-31'
                                                    ) CIURR
                                     on cp.corporate_item_integration_id = CIURR.CORPORATE_ITEM_INTEGRATION_ID and cp.ROG_ID = CIURR.ROG_ID 
                                         and cp.UPC_NBR = CIURR.UPC_NBR
                                     left outer join 
                                                    (select distinct * from EDM_CONFIRMED_PRD.DW_C_PRODUCT.Pending_Retail_Item_Price 
                                                     where DW_Last_Effective_Dt = '9999-12-31' and DW_LOGICAL_DELETE_IND = False
                                                     and datediff(day,CURRENT_DATE(), Price_Effective_Start_Dt)  >=  0
                                                     and datediff(day,CURRENT_DATE(), Price_Effective_Start_Dt)  <  4
                                                     ) pp
                                     on cp.rog_id = pp.rog_id and cp.upc_nbr = pp.upc_nbr and cp.Price_Area_Cd = pp.Price_Area_Cd
                                     left outer join (select distinct ROG_ID,DIVISION_ID from EDM_CONFIRMED_PRD.DW_C_LOCATION.RETAIL_ORDER_GROUP_DIVISION 
                                                      where DW_Last_Effective_Dt = '9999-12-31') rogdiv
                                     on cp.rog_id=rogdiv.ROG_ID
                                     left outer join (select distinct * from 
                                                EDM_CONFIRMED_PRD.DW_C_PRODUCT.RETAIL_ORDER_GROUP_UPC where DW_Last_Effective_Dt = '9999-12-31') rogupc
                                     on cp.rog_id = rogupc.rog_id and cp.upc_nbr = rogupc.upc_nbr
                                     inner join (select distinct upcid,areacd,rogcd, max(createtime) createtime, max(modifiedtime) modifiedtime
                                                    from EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_ROGPRICE_CHANGE
                                                    group by upcid,areacd,rogcd) rpc
                                     on cp.rog_id = rpc.rogCd  and cp.upc_nbr = rpc.upcid and cp.Price_Area_Cd = rpc.areaCd
                                     where 
                                     cp.DW_Last_Effective_Dt = '9999-12-31'    and cp.DW_LOGICAL_DELETE_IND = False and                                        
                                       nvl(rpc.createtime,rpc.modifiedtime)>=DATEADD('HOUR',-24,CURRENT_TIMESTAMP)) src
                                     left join (                                            
                                      select * from (                                                       
                                                                       select t.*, row_number()
                                                                        over (partition by rogcd,upcId,priceAreaCd order by MODIFIEDTIME desc ) as rn
                                                                        from EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_ROGPRICE t) where rn=1
                                                      ) tgt 
                                                     on src.upcid=tgt.upcid and src.rogCd=tgt.rogCd and src.priceAreaCd = tgt.priceAreaCd 
                                             where (tgt.upcid is null 
                                                   and tgt.rogCd is null
                                                   and tgt.priceAreaCd is null
                                                   and src.deleteYN = FALSE
                                                   )),Table(EDM_CONFIRMED_OUT_PRD.DW_DCAT.validateCurrentPrice(
                                    retailsectioncd,
                                    rogCd,
                                    upcId,
                                    divisionId,
                                    priceAreaCd,
                                    upTblNum,
                                    upMeasure,
                                    upMeasureUnit,
                                    upLabelUnit,
                                    upMultFctr,
                                    itemPriceMethodCd,
                                    itemPriceAmt,
                                    itemPriceLimtQty,
                                    itemPriceFactor,
                                    itemPriceAlternatePriceAmt,
                                    itemPriceAlternatePriceFactor,
                                    regularRetailItemPriceAmt,
                                    regularRetailItemPriceFactor,
                                    regularRetailItemAltPriceAmt,
                                    regularRetailItemAltPriceFactor,
                                    regularItemLimitQty,
                                    regularPriceMethodCd,
                                    reglarPriceChangeDt,
                                    pendingPricePriceMethodCd,
                                    pendingPriceItemLimtQty,
                                    pendingPriceItemPriceAmt,
                                    pendingPriceItemPriceFactor,
                                    pendingPriceAlternatePriceAmt,
                                    pendingPriceAlternatePriceFactor,
                                    pendingPriceEffectiveStartDt,
                                    pendingPriceEffectiveEndDt,
                                    pendingPriceLongTermSpecialLnd,
                                    deleteYN))
                            WHERE valid = true
							
							
--STOREPRICE

                    SELECT upcid, 
                 divisionid, 
                 rogcd, 
                 priceareacd, 
                 uptblnum, 
                 upmeasure, 
                 upmeasureunit, 
                 uplabelunit, 
                 upmultfctr, 
                 storeId, 
                 storeitempriceeffectivestartdt, 
                 storeitempriceeffectiveenddt, 
                 storeitempricemethodcd, 
                 storeitempriceitemlimtqty, 
                 storeitempriceitempriceamt, 
                 storeitempriceitempricefactor, 
                 storeitempricealternatepriceamt, 
                 storeitempricealternatepricefactor, 
                 storeitempricepriceoverrideind, 
                 deleteYN,
                 DW_SOURCE_CREATE_NM,
                 createtime, 
                 modifiedtime FROM (                       SELECT CAST(src.upcid AS FLOAT) AS upcid, 
                     CAST(src.divisionid AS VARCHAR(16777216)) AS divisionid, 
                     CAST(src.rogcd AS VARCHAR(16777216)) AS rogcd, 
                     CAST(src.priceareacd AS VARCHAR(16777216)) AS priceareacd, 
                     CAST(src.uptblnum AS FLOAT) AS uptblnum, 
                     CAST(src.upmeasure AS FLOAT) AS upmeasure, 
                     CAST(src.upmeasureunit AS VARCHAR(16777216)) AS upmeasureunit, 
                     CAST(src.uplabelunit AS VARCHAR(16777216)) AS uplabelunit, 
                     CAST(src.upmultfctr AS FLOAT) AS upmultfctr, 
                     CAST(src.storeId AS VARCHAR(16777216)) AS storeId, 
                     CAST(src.storeitempriceeffectivestartdt AS DATE) AS storeitempriceeffectivestartdt, 
                     CAST(src.storeitempriceeffectiveenddt AS DATE) AS storeitempriceeffectiveenddt, 
                     CAST(src.storeitempricemethodcd AS VARCHAR(16777216)) AS storeitempricemethodcd, 
                     CAST(src.storeitempriceitemlimtqty AS FLOAT) AS storeitempriceitemlimtqty, 
                     CAST(src.storeitempriceitempriceamt AS FLOAT) AS storeitempriceitempriceamt, 
                     CAST(src.storeitempriceitempricefactor AS FLOAT) AS storeitempriceitempricefactor, 
                     CAST(src.storeitempricealternatepriceamt AS FLOAT) AS storeitempricealternatepriceamt, 
                     CAST(src.storeitempricealternatepricefactor AS FLOAT) AS storeitempricealternatepricefactor, 
                     CAST(src.storeitempricepriceoverrideind AS VARCHAR(16777216)) AS storeitempricepriceoverrideind,
                     src.deleteYN,
                     src.DW_SOURCE_CREATE_NM,
                     DW_CREATE_TS as createtime , 
                     NVL(DW_LAST_UPDATE_TS,DW_CREATE_TS) as modifiedtime
                      FROM (SELECT sp.upc_nbr as upcid, 
                       f.division_id as divisionid, 
                       rs.rog_id as rogcd, 
                       '' AS priceAreaCd, 
                       rogupc.UNIT_PRICE_TABLE_NBR as upTblNum,
                       rogupc.unit_Price_Measure_Nbr  as  upMeasure, 
                       rogupc.Unit_Price_Measure_Unit as upMeasureUnit,
                       rogupc.UNIT_PRICE_LABEL_UNIT_CD as upLabelUnit,
                       rogupc.UNIT_PRICE_MULTIPLICATION_FCTR as upMultFctr,
                       f.facility_nbr as storeid, 
                       price_effective_start_dt as storeitempriceeffectivestartdt, 
                       price_effective_end_dt as storeitempriceeffectiveenddt , 
                       price_method_cd as storeitempricemethodcd, 
                       item_limit_qty as storeitempriceitemlimtqty , 
                       item_price_amt as storeitempriceitempriceamt, 
                       item_price_fctr as storeitempriceitempricefactor, 
                       alternate_price_amt as storeitempricealternatepriceamt , 
                       alternate_price_fctr as storeitempricealternatepricefactor , 
                       price_override_ind as storeitempricepriceoverrideind,
                       sp.DW_LOGICAL_DELETE_IND as deleteYN,
                       sp.DW_SOURCE_CREATE_NM,
                       sp.DW_CREATE_TS,
                       sp.DW_LAST_UPDATE_TS
                        FROM   EDM_CONFIRMED_PRD.DW_C_PRODUCT.Store_Retail_Item_Price sp 
                               LEFT JOIN (select distinct facility_integration_id, rog_id,banner_nm from EDM_CONFIRMED_PRD.dw_c_location.retail_store 
                                          where DW_Last_Effective_Dt = '9999-12-31') rs 
                                      ON sp.facility_integration_id = rs.facility_integration_id
                               LEFT JOIN (select distinct facility_integration_id, facility_nbr,division_id from EDM_CONFIRMED_PRD.dw_c_location.facility 
                                          where DW_Last_Effective_Dt = '9999-12-31'
                                          ) f
                                      ON sp.facility_integration_id = f.facility_integration_id
                               left join (select distinct * from 
                                                EDM_CONFIRMED_PRD.DW_C_PRODUCT.RETAIL_ORDER_GROUP_UPC where DW_Last_Effective_Dt = '9999-12-31') rogupc
                                            on rs.rog_id = rogupc.rog_id and sp.upc_nbr = rogupc.upc_nbr
                        WHERE   sp.DW_Last_Effective_Dt = '9999-12-31'
                                 AND rs.rog_id in (select distinct ROGCD 
                                                    FROM ECATALOG_ROG_CONTROL
                                                    WHERE FULLLOADFLAG = TRUE) --BASED on request from digital catalog team
                                AND datediff(day,CURRENT_DATE(),sp.price_effective_end_dt)  > 0
                                AND datediff(day,CURRENT_DATE(),sp.price_effective_start_dt) < 4 
                                ) src
                                left join 
                           ( select * from ( select t.*, row_number()
                                         over (partition by upcid,storeid,storeitempriceeffectivestartdt,storeitempriceeffectiveenddt order by MODIFIEDTIME desc ) as rn
                                         from EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_STOREPRICE t) where rn=1) tgt 
                                         on src.upcid=tgt.upcid  and src.storeid=tgt.storeid 
                                             and src.storeitempriceeffectivestartdt=tgt.storeitempriceeffectivestartdt and src.storeitempriceeffectiveenddt=tgt.storeitempriceeffectiveenddt
                                          where (tgt.upcid is null 
                                                and tgt.storeid is null
                                                and tgt.storeitempriceeffectivestartdt is null
                                                and tgt.storeitempriceeffectiveenddt is null
                                                )),Table(EDM_CONFIRMED_OUT_PRD.DW_DCAT.validateEcatStorePrice( upcid, 
                 divisionid, 
                 rogcd, 
                 priceareacd, 
                 uptblnum, 
                 upmeasure, 
                 upmeasureunit, 
                 uplabelunit, 
                 upmultfctr, 
                 storeId, 
                 storeitempriceeffectivestartdt, 
                 storeitempriceeffectiveenddt, 
                 storeitempricemethodcd, 
                 storeitempriceitemlimtqty, 
                 storeitempriceitempriceamt, 
                 storeitempriceitempricefactor, 
                 storeitempricealternatepriceamt, 
                 storeitempricealternatepricefactor, 
                 storeitempricepriceoverrideind,
                 deleteYN
                 ))
            WHERE valid = true
			
--RETAIL STORE PRICEAREA

SELECT    CASE WHEN new.rogCd is NULL THEN old.rogCd ELSE new.rogCd END AS rogCd, 
                    CASE WHEN new.STOREID is NULL THEN old.STOREID ELSE new.STOREID END AS STOREID,
                    CASE WHEN new.RETAILSECT is NULL THEN old.RETAILSECT ELSE new.RETAILSECT END AS RETAILSECT,
                    --CASE WHEN new.priceArea is NULL THEN old.priceArea ELSE new.priceArea END AS priceArea,
                    --CASE WHEN new.rogCd is NULL THEN 'Y' ELSE 'N' END AS deleteYN,
                    CASE WHEN new.rogCd is NULL THEN 'Delete' ELSE 'Insert' END AS actionItem,DW_CREATE_TS_SRC
            FROM    (    SELECT     DISTINCT rogCd, STOREID,RETAILSECT,DW_CREATE_TS_SRC
                        FROM    (SELECT    DISTINCT
                       Retail_store.ROG_ID   as rogCd,
                       lpad(Facility.Facility_Nbr,4,'0') as STOREID,
                       Retail_Store_Section_Price_Area.section_cd as RETAILSECT,
                       Retail_Store_Section_Price_Area.price_Area_Cd as priceArea
                      ,NVL(Retail_Store_Section_Price_Area.DW_SOURCE_UPDATE_NM,Retail_Store_Section_Price_Area.DW_SOURCE_CREATE_NM) AS DW_SOURCE_CREATE_NM
                      ,CURRENT_TIMESTAMP as DW_CREATE_TS 
                      ,false as DW_LOGICAL_DELETE_IND
                    ,CASE     WHEN Retail_Store_Section_Price_Area.price_Area_Cd is NULL THEN 'priceArea is NULL'
                            ELSE NULL
                    END    AS EXCEP_FLAG,Retail_store.DW_CREATE_TS AS DW_CREATE_TS_SRC              
            FROM    EDM_CONFIRMED_PRD.DW_C_LOCATION.Retail_store Retail_store
            JOIN    EDM_CONFIRMED_PRD.DW_C_LOCATION.facility    ON    facility.facility_integration_id = retail_store.facility_integration_id
                                                                                            AND    facility.DW_CURRENT_VERSION_IND = TRUE
                                                                                            AND    facility.DW_LOGICAL_DELETE_IND = FALSE
            JOIN    EDM_CONFIRMED_PRD.DW_C_LOCATION.Retail_Store_Section_Price_Area         ON     Retail_Store_Section_Price_Area.FACILITY_INTEGRATION_ID = facility.FACILITY_INTEGRATION_ID
                                                                                AND Retail_Store_Section_Price_Area.DW_CURRENT_VERSION_IND = TRUE
                                                                                AND    Retail_Store_Section_Price_Area.DW_LOGICAL_DELETE_IND = FALSE
            WHERE        RETAIL_STORE.DW_CURRENT_VERSION_IND = TRUE
            AND            Retail_store.DW_LOGICAL_DELETE_IND = FALSE
            AND        Retail_store.Rog_Id in (SELECT DISTINCT ROGCD FROM EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_ROG_CONTROL WHERE INCREMENTLOADFLAG = TRUE))
                        where excep_flag is null
                    ) new
            FULL OUTER JOIN (    SELECT     DISTINCT rogCd, STOREID,RETAILSECT
                                FROM    EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_RETAIL_STORE_PRICE_AREA 
                                WHERE     DW_LOGICAL_DELETE_IND = FALSE
                            ) old
                            ON    old.rogCd = new.rogCd 
                            AND    old.STOREID = new.STOREID
                            AND old.retailsect =  new.retailsect
            WHERE    (new.rogCd IS NULL  and   new.STOREID is NULL and new.RETAILSECT is NULL ) or
            (old.rogCd IS NULL and old.storeid is null and old.retailsect is null )
			
			
			
			
--PROMO	(COUPON DATA)		
  
			
			SELECT     banner,     
                                           division,     
                                           rogcd,     
                                           storeid,     
                                           upcid,     
                                           couponamt,     
                                           originalcouponfactor,     
                                           originalcouponamt,     
                                           couponmethodcd,     
                                           promotionstartdt,     
                                           promotionenddt,     
                                           minimumpurchaseqty,     
                                           couponlimitqty,     
                                           linkplunbr,     
                                           couponPLUCd,
                                           bibtypecd,     
                                           bibtagnbr,     
                                           bibtagverbiage, 
                                           deleteYN,    
                                           DW_SOURCE_CREATE_NM,
                                           createTime,    
                                           modifiedtime,
                                           promotionTypeCd
                                   FROM  (select 
                                   
                                   CAST(src.banner AS VARCHAR(16777216)) AS banner,     
                                           CAST(src.division AS VARCHAR(16777216)) AS division,     
                                           CAST(src.rogcd AS VARCHAR(16777216)) AS rogcd,     
                                           CAST(src.storeid AS VARCHAR(16777216)) AS storeid,     
                                           CAST(src.upcid AS FLOAT) AS upcid,     
                                           CAST(src.couponamt AS FLOAT) AS couponamt,     
                                           CAST(src.originalcouponfactor AS FLOAT) AS originalcouponfactor,     
                                           CAST(src.originalcouponamt AS FLOAT) AS originalcouponamt,     
                                           CAST(src.couponmethodcd AS VARCHAR(16777216)) AS couponmethodcd,     
                                           CAST(src.promotionstartdt AS DATE) AS promotionstartdt,     
                                           CAST(src.promotionenddt AS DATE) AS promotionenddt,     
                                           CAST(src.minimumpurchaseqty AS FLOAT) AS minimumpurchaseqty,     
                                           CAST(src.couponlimitqty AS FLOAT) AS couponlimitqty,     
                                           CAST(src.linkplunbr AS FLOAT) AS linkplunbr,     
                                           CAST(src.couponPLUCd AS VARCHAR(16777216)) AS couponPLUCd,
                                           CAST(src.bibtypecd AS VARCHAR(16777216)) AS bibtypecd,     
                                           CAST(src.bibtagnbr AS VARCHAR(16777216)) AS bibtagnbr,     
                                           CAST(src.bibtagverbiage AS VARCHAR(16777216)) AS bibtagverbiage, 
                                           CAST(src.deleteYN AS BOOLEAN) AS deleteYN,    
                                           CAST(src.DW_SOURCE_CREATE_NM AS VARCHAR(16777216)) AS DW_SOURCE_CREATE_NM,
                                           

                                                          DW_CREATE_TS as createtime , 
                                                          NVL(DW_LAST_UPDATE_TS,DW_CREATE_TS) as modifiedtime,
                                                          CAST(src.promotionTypeCd AS VARCHAR(16777216)) AS promotionTypeCd
                                                    from 
                                                    (SELECT distinct rs.banner_nm as banner, 
                                                           f.division_id as division, 
                                                           rs.rog_id as rogcd, 
                                                           f.facility_nbr as storeid, 
                                                           ps.upc_nbr as upcid, 
                                                           ps.coupon_amt as couponamt, 
                                                           ps.original_coupon_fctr as originalcouponfactor, 
                                                           ps.original_coupon_amt as originalcouponamt, 
                                                           ps.coupon_method_cd as couponmethodcd, 
                                                           ps.promotion_start_dt as promotionstartdt, 
                                                           ps.promotion_end_dt as promotionenddt, 
                                                           ps.minimum_purchase_qty as minimumpurchaseqty, 
                                                           ps.coupon_limit_qty as couponlimitqty, 
                                                           ps.link_plu_nbr as linkplunbr,
                                                           ps.COUPON_PLU_CD as couponplucd,
                                                           ps.bib_type_cd as bibtypecd, 
                                                           ps.bib_tag_nbr as bibtagnbr, 
                                                           '' AS bibTagVerbiage,
                                                           ps.DW_LOGICAL_DELETE_IND as deleteYN,
                                                           ps.DW_SOURCE_CREATE_NM,
                                                           DW_CREATE_TS,
                                                           DW_LAST_UPDATE_TS,
                                                           ps.PROMOTION_TYPE_CD as promotionTypeCd  
                                                    FROM   (                                      select
                                                                          PROMOTION_STORE_INTEGRATION_ID,
                                                                        DW_FIRST_EFFECTIVE_DT,
                                                                        DW_LAST_EFFECTIVE_DT,
                                                                        FACILITY_INTEGRATION_ID,
                                                                        UPC_NBR,
                                                                        LINK_PLU_NBR,
                                                                        COUPON_PLU_CD,
                                                                        PROMOTION_START_DT,
                                                                        PROMOTION_END_DT,
                                                                        PACS_ADPLAN_SEQUENCE_NBR,
                                                                        COUPON_TYPE_CD,
                                                                        COUPON_AMT,
                                                                        COUPON_METHOD_CD,
                                                                        COUPON_LIMIT_QTY,
                                                                        ORIGINAL_COUPON_AMT,
                                                                        ORIGINAL_COUPON_FCTR,
                                                                        ORIGINAL_COUPON_METHOD_CD,
                                                                        COUPON_ADJUSTED_IND,
                                                                        COUPON_DSC,
                                                                        COUPON_PAGE_NBR,
                                                                        COUPON_ORIGIN_CD,
                                                                        COUPON_BOOK_IND,
                                                                        PROMOTION_CD,
                                                                        PROMOTION_PRICE_AMT,
                                                                        PRICE_FCTR,
                                                                        PRICING_METHOD_CD,
                                                                        COMMON_CD_USED_IND,
                                                                        MINIMUM_PURCHASE_QTY,
                                                                        MINIMUM_PURCHASE_AMT,
                                                                        MINIMUM_PURCHASE_IND,
                                                                        BIB_TYPE_CD,
                                                                        BIB_TAG_NBR,
                                                                        SIGN_HEADER_ID,
                                                                        SIGN_TYPE_CD,
                                                                        SIGN_1_CNT,
                                                                        SIGN_2_CNT,
                                                                        SIGN_3_CNT,
                                                                        POS_PROCESSED_IND,
                                                                        LIMIT_BY_WEIGHT_IND,
                                                                        FUNDED_IND,
                                                                        FUNDED_AMT,
                                                                        PRICE_REQUIRED_IND,
                                                                        FRANKING_IND,
                                                                        OPEN_DRAWER_IND,
                                                                        PERFORMANCE_DETAIL_SOURCE_ID,
                                                                        PERFORMANCE_DETAIL_ID,
                                                                        COPIENT_IND,
                                                                        PROMOTION_TYPE_CD,
                                                                        ALTERNATE_PRICE_AMT,
                                                                        ALTERNATE_PRICE_FCTR,
                                                                        LIMITED_QUANTITY_NBR,
                                                                        OVERRIDE_PRICE_IND,
                                                                        PRICE_REASON_CD,
                                                                        DW_CREATE_TS,
                                                                        DW_LAST_UPDATE_TS,
                                                                        DW_LOGICAL_DELETE_IND,
                                                                        DW_SOURCE_CREATE_NM,
                                                                        DW_SOURCE_UPDATE_NM,
                                                                        DW_CURRENT_VERSION_IND 
                                       from EDM_CONFIRMED_PRD.DW_C_PRODUCT.Promotion_Store) ps 
                                                           LEFT JOIN (select distinct facility_integration_id, rog_id,banner_nm from EDM_CONFIRMED_PRD.dw_c_location.retail_store 
                                                                        where DW_Last_Effective_Dt = '9999-12-31') rs
                                                                  ON ps.facility_integration_id = rs.facility_integration_id
                                                           LEFT JOIN (select distinct facility_integration_id, facility_nbr,division_id from EDM_CONFIRMED_PRD.dw_c_location.facility 
                                                                      where DW_Last_Effective_Dt = '9999-12-31') f 
                                                                  ON ps.facility_integration_id = f.facility_integration_id
                                                    WHERE  ps.DW_Last_Effective_Dt = '9999-12-31'
                                                           AND ps.PROMOTION_TYPE_CD = 'CS' 
                                                           AND rs.rog_id in (select distinct ROGCD 
                                                                FROM ECATALOG_ROG_CONTROL
                                                                WHERE FULLLOADFLAG = TRUE) --BASED on request from digital catalog team
                                                           AND datediff(day,CURRENT_DATE(),ps.promotion_end_dt) >= 0
                                                           AND (datediff(day,CURRENT_DATE(),ps.promotion_start_dt)) < 4
                                                    ORDER BY DW_LAST_UPDATE_TS,DW_CREATE_TS
                                                    ) src
                                                    left join                                                             
                                                    (select * from 
                                                    (select t.*, row_number()
                                                    over (partition by upcid,storeid,promotionStartDt,promotionEndDt,linkPLUNbr,couponPLUCd order by MODIFIEDTIME desc ) as rn
                                                    from EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_PROMO t where promotionTypeCd = 'CS') 
                                                    where rn=1) tgt
                                                        on src.upcid=tgt.upcid and src.storeid=tgt.storeid
                                                        and src.promotionStartDt=tgt.promotionStartDt and src.promotionEndDt = tgt.promotionEndDt
                                                        and src.linkPLUNbr=tgt.linkPLUNbr and src.couponPLUCd = tgt.couponPLUCd
                                                        where (tgt.upcid is null 
                                                        and tgt.storeid is null
                                                        and tgt.promotionStartDt is null
                                                        and tgt.promotionEndDt is null
                                                        and tgt.linkPLUNbr is null
                                                        and tgt.couponPLUCd is null
                                                        and src.deleteYN = False
                                                        )
                                                        OR (
                                                        tgt.upcid is not null
                                                        and (  
                                                        nvl(src.banner,'-1')   <> nvl(tgt.banner,'-1') 
                                                        OR nvl(src.division,'-1')   <> nvl(tgt.division,'-1')  
                                                        OR nvl(src.rogcd,'-1')   <> nvl(tgt.rogcd,'-1')  
                                                        OR nvl(src.couponamt,'-1')   <> nvl(tgt.couponamt ,'-1') 
                                                        OR nvl(src.originalcouponfactor,'-1')   <> nvl(tgt.originalcouponfactor ,'-1') 
                                                        OR nvl(src.originalcouponamt,'-1')   <> nvl(tgt.originalcouponamt ,'-1') 
                                                        OR nvl(src.couponmethodcd,'-1')   <> nvl(tgt.couponmethodcd ,'-1') 
                                                        OR nvl(src.minimumpurchaseqty,'-1')   <> nvl(tgt.minimumpurchaseqty ,'-1') 
                                                        OR nvl(src.couponlimitqty,'-1')   <> nvl(tgt.couponlimitqty,'-1')  
                                                        OR nvl(src.bibtypecd,'-1')   <> nvl(tgt.bibtypecd ,'-1') 
                                                        OR nvl(src.bibtagnbr,'-1')   <> nvl(tgt.bibtagnbr ,'-1') 
                                                        OR nvl(src.bibtagverbiage,'-1')   <> nvl(tgt.bibtagverbiage,'-1') 
                                                        OR src.deleteYN  <> tgt.deleteYN 
                                                        ))),     
                                   Table(EDM_CONFIRMED_OUT_PRD.DW_DCAT.validateEcatPromo(banner, division, rogcd, storeid, upcid,    
                                         couponamt,     
                                               originalcouponfactor, originalcouponamt, couponmethodcd,    
                                         promotionstartdt,     
                                               promotionenddt, minimumpurchaseqty, couponlimitqty,    
                                         linkplunbr,     
                                         bibtypecd,     
                                         bibtagnbr, bibtagverbiage,promotionTypeCd,deleteYN))     
                            WHERE valid = true
							
--PROMO (NON-COUPON DATA)

                                select 
                                       banner, 
                                       division, 
                                       rogcd, 
                                       storeid, 
                                       upcid, 
                                       deleteYN,
                                       promotionstartdt, 
                                       promotionenddt, 
                                       couponmethodcd,
                                       limitedQuantityNbr,
                                       originalCouponAmt, 
                                       originalCouponFactor,
                                       alternatePriceAmt,
                                       alternatePriceFctr,
                                       priceOverrideInd,
                                       DW_SOURCE_CREATE_NM,
                                       createtime,
                                       modifiedtime,
                                       promotionTypeCd
                            from (select 

                                   CAST(src.banner AS VARCHAR(16777216)) AS banner,     
                                           CAST(src.division AS VARCHAR(16777216)) AS division,     
                                           CAST(src.rogcd AS VARCHAR(16777216)) AS rogcd,     
                                           CAST(src.storeid AS VARCHAR(16777216)) AS storeid,     
                                           CAST(src.upcid AS FLOAT) AS upcid,     
                                           CAST(src.deleteYN AS BOOLEAN) AS deleteYN,    
                                           CAST(src.promotionstartdt AS DATE) AS promotionstartdt,     
                                           CAST(src.promotionenddt AS DATE) AS promotionenddt, 
                                           CAST(src.couponmethodcd AS VARCHAR(16777216)) AS couponmethodcd,
                                           CAST(src.limitedQuantityNbr AS FLOAT) AS limitedQuantityNbr,                                           
                                           CAST(src.originalcouponamt AS FLOAT) AS originalcouponamt,
                                           CAST(src.originalcouponfactor AS FLOAT) AS originalcouponfactor,                                                 
                                           CAST(src.alternatePriceAmt AS FLOAT) AS alternatePriceAmt,
                                           CAST(src.alternatePriceFctr AS FLOAT) AS alternatePriceFctr,
                                           CAST(src.priceOverrideInd AS VARCHAR(16777216)) AS priceOverrideInd,                                       
                                           CAST(src.DW_SOURCE_CREATE_NM AS VARCHAR(16777216)) AS DW_SOURCE_CREATE_NM,
                                           DW_CREATE_TS as createtime, 
                                           NVL(DW_LAST_UPDATE_TS,DW_CREATE_TS) as modifiedtime,
                                           CAST(src.promotionTypeCd AS VARCHAR(16777216)) AS promotionTypeCd

                                                    from 
                                                    (SELECT distinct rs.banner_nm as banner, 
                                                          f.division_id as division, 
                                                           rs.rog_id as rogcd, 
                                                           f.facility_nbr as storeid, 
                                                           ps.upc_nbr as upcid, 
                                                           ps.DW_LOGICAL_DELETE_IND as deleteYN,
                                                           ps.promotion_start_dt as promotionstartdt, 
                                                           ps.promotion_end_dt as promotionenddt, 
                                                           ps.pricing_method_cd as couponmethodcd,
                                                           ps.limited_Quantity_Nbr as limitedQuantityNbr,
                                                           ps.PROMOTION_PRICE_AMT as originalcouponamt, 
                                                           ps.PRICE_FCTR as originalcouponfactor, 
                                                           ps.ALTERNATE_PRICE_AMT as alternatePriceAmt,
                                                           ps.ALTERNATE_PRICE_FCTR as alternatePriceFctr,
                                                           ps.OVERRIDE_PRICE_IND as priceOverrideInd,
                                                           ps.DW_SOURCE_CREATE_NM,
                                                           DW_CREATE_TS,
                                                           DW_LAST_UPDATE_TS,
                                                           ps.PROMOTION_TYPE_CD as promotionTypeCd  
                                                    FROM   (select
                                                                          PROMOTION_STORE_INTEGRATION_ID,
                                                                        DW_FIRST_EFFECTIVE_DT,
                                                                        DW_LAST_EFFECTIVE_DT,
                                                                        FACILITY_INTEGRATION_ID,
                                                                        UPC_NBR,
                                                                        LINK_PLU_NBR,
                                                                        COUPON_PLU_CD,
                                                                        PROMOTION_START_DT,
                                                                        PROMOTION_END_DT,
                                                                        PACS_ADPLAN_SEQUENCE_NBR,
                                                                        COUPON_TYPE_CD,
                                                                        COUPON_AMT,
                                                                        COUPON_METHOD_CD,
                                                                        COUPON_LIMIT_QTY,
                                                                        ORIGINAL_COUPON_AMT,
                                                                        ORIGINAL_COUPON_FCTR,
                                                                        ORIGINAL_COUPON_METHOD_CD,
                                                                        COUPON_ADJUSTED_IND,
                                                                        COUPON_DSC,
                                                                        COUPON_PAGE_NBR,
                                                                        COUPON_ORIGIN_CD,
                                                                        COUPON_BOOK_IND,
                                                                        PROMOTION_CD,
                                                                        PROMOTION_PRICE_AMT,
                                                                        PRICE_FCTR,
                                                                        PRICING_METHOD_CD,
                                                                        COMMON_CD_USED_IND,
                                                                        MINIMUM_PURCHASE_QTY,
                                                                        MINIMUM_PURCHASE_AMT,
                                                                        MINIMUM_PURCHASE_IND,
                                                                        BIB_TYPE_CD,
                                                                        BIB_TAG_NBR,
                                                                        SIGN_HEADER_ID,
                                                                        SIGN_TYPE_CD,
                                                                        SIGN_1_CNT,
                                                                        SIGN_2_CNT,
                                                                        SIGN_3_CNT,
                                                                        POS_PROCESSED_IND,
                                                                        LIMIT_BY_WEIGHT_IND,
                                                                        FUNDED_IND,
                                                                        FUNDED_AMT,
                                                                        PRICE_REQUIRED_IND,
                                                                        FRANKING_IND,
                                                                        OPEN_DRAWER_IND,
                                                                        PERFORMANCE_DETAIL_SOURCE_ID,
                                                                        PERFORMANCE_DETAIL_ID,
                                                                        COPIENT_IND,
                                                                        PROMOTION_TYPE_CD,
                                                                        ALTERNATE_PRICE_AMT,
                                                                        ALTERNATE_PRICE_FCTR,
                                                                        LIMITED_QUANTITY_NBR,
                                                                        OVERRIDE_PRICE_IND,
                                                                        PRICE_REASON_CD,
                                                                        DW_CREATE_TS,
                                                                        DW_LAST_UPDATE_TS,
                                                                        DW_LOGICAL_DELETE_IND,
                                                                        DW_SOURCE_CREATE_NM,
                                                                        DW_SOURCE_UPDATE_NM,
                                                                        DW_CURRENT_VERSION_IND 
                                       from EDM_CONFIRMED_PRD.DW_C_PRODUCT.Promotion_Store) ps 
                                                           LEFT JOIN (select distinct facility_integration_id, rog_id,banner_nm from EDM_CONFIRMED_PRD.dw_c_location.retail_store 
                                                                        where DW_Last_Effective_Dt = '9999-12-31') rs
                                                                  ON ps.facility_integration_id = rs.facility_integration_id
                                                           LEFT JOIN (select distinct facility_integration_id, facility_nbr,division_id from EDM_CONFIRMED_PRD.dw_c_location.facility 
                                                                      where DW_Last_Effective_Dt = '9999-12-31') f 
                                                                  ON ps.facility_integration_id = f.facility_integration_id
                                                    WHERE  ps.DW_Last_Effective_Dt = '9999-12-31'
                                                           AND ps.PROMOTION_TYPE_CD = 'AS' 
                                                           AND rs.rog_id in (select distinct ROGCD 
                                                                FROM ECATALOG_ROG_CONTROL
                                                                WHERE FULLLOADFLAG = TRUE) --BASED on request from digital catalog team
                                                           AND datediff(day,CURRENT_DATE(),ps.promotion_end_dt) >= 0
                                                           AND (datediff(day,CURRENT_DATE(),ps.promotion_start_dt)) < 4
                                                    ORDER BY DW_LAST_UPDATE_TS,DW_CREATE_TS
                                                    ) src
                                                    left join                                                             
                                                    (select * from 
                                                    (select t.*, row_number()
                                                    over (partition by upcid,storeid,promotionStartDt,promotionEndDt order by MODIFIEDTIME desc ) as rn
                                                    from EDM_CONFIRMED_OUT_PRD.DW_DCAT.ECATALOG_PROMO t where promotionTypeCd = 'AS') 
                                                    where rn=1) tgt
                                                        on src.upcid=tgt.upcid and src.storeid=tgt.storeid
                                                        and src.promotionStartDt=tgt.promotionStartDt and src.promotionEndDt = tgt.promotionEndDt
                                                        where (tgt.upcid is null 
                                                        and tgt.storeid is null
                                                        and tgt.promotionStartDt is null
                                                        and tgt.promotionEndDt is null
                                                        and src.deleteYN = False
                                                        )
                                                        OR (
                                                        tgt.upcid is not null
                                                        and (  
                                                        nvl(src.banner,'-1')  <> nvl(tgt.banner,'-1')
                                                        OR nvl(src.division,'-1')  <> nvl(tgt.division ,'-1')
                                                        OR nvl(src.rogcd ,'-1') <> nvl(tgt.rogcd ,'-1')
                                                        OR nvl(src.originalcouponfactor,'-1')  <> nvl(tgt.originalcouponfactor ,'-1')
                                                        OR nvl(src.originalcouponamt,'-1')  <> nvl(tgt.originalcouponamt,'-1') 
                                                        OR nvl(src.couponmethodcd,'-1')  <> nvl(tgt.couponmethodcd,'-1') 
                                                        OR nvl(src.limitedQuantityNbr,'-1') <> nvl(tgt.limitedQuantityNbr,'-1')
                                                        OR nvl(src.alternatePriceAmt,'-1') <> nvl(tgt.alternatePriceAmt,'-1')
                                                        OR nvl(src.alternatePriceFctr,'-1') <> nvl(tgt.alternatePriceFctr,'-1')
                                                        OR nvl(src.priceOverrideInd,'-1') <> nvl(tgt.priceOverrideInd,'-1')
                                                        OR nvl(src.deleteYN,'-1') <> nvl(tgt.deleteYN,'-1')
                                                        ))),
                            Table(EDM_CONFIRMED_OUT_PRD.DW_DCAT.validateEcatNonCouponPromo(
                              banner,
                              division,
                              rogCd,
                              storeId,
                              upcId,
                              promotionStartDt,
                              promotionEndDt,
                              couponMethodCd,
                              limitedQuantityNbr,
                              originalCouponAmt,
                              alternatePriceAmt,
                              deleteYN))
                              WHERE valid = true
							  
--STORE ITEM

Select distinct Retail_Store_UPC.UPC_Nbr, CAST(CAST(lpad(facility.facility_nbr,4,0) AS NUMERIC) AS STRING) facility_nbr
FROM "EDM_CONFIRMED_PRD"."DW_C_PRODUCT"."RETAIL_STORE_UPC" Retail_Store_UPC
JOIN "EDM_CONFIRMED_PRD"."DW_C_LOCATION"."FACILITY" facility
    ON    Facility.Facility_Integration_Id = Retail_Store_UPC.Facility_Integration_Id
JOIN(    
    SELECT DISTINCT rogCd, upcId FROM "EDM_CONFIRMED_OUT_PRD"."DW_DCAT".ECATALOG_ITEM
    WHERE DW_LOGICAL_DELETE_IND = FALSE
) Item     
    ON    Item.upcId = Retail_Store_UPC.UPC_Nbr
        AND    Item.rogCd    = Retail_Store_UPC.Rog_Id
where Retail_Store_UPC.DW_CURRENT_VERSION_IND = TRUE
    AND Retail_Store_UPC.DW_LOGICAL_DELETE_IND = FALSE
    AND DATE(NVL(Retail_Store_UPC.DW_LAST_UPDATE_TS,Retail_Store_UPC.DW_CREATE_TS)) >= DATEADD(DAY,-2,CURRENT_DATE())
    AND DATE(NVL(Retail_Store_UPC.DW_LAST_UPDATE_TS,Retail_Store_UPC.DW_CREATE_TS)) <= DATEADD('HOUR',-1,CURRENT_TIMESTAMP())    
MINUS
Select * from(
    Select distinct upcId, storeId
    FROM "EDM_CONFIRMED_OUT_PRD"."DW_DCAT"."ECATALOG_STORE_ITEM"
    UNION ALL
    Select distinct upcId, storeId
    from "EDM_CONFIRMED_OUT_PRD"."DW_DCAT"."ECATALOG_STORE_ITEM_EXCEPTIONS"
)