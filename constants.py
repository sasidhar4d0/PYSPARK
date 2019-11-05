__author__ = 'vn50fti'

at_table_map = {'US': {0: 'select * from spine_poc.us_wm_skp_tables_skp_item_hierarchy',
                       3: "select * from spine_poc.us_wm_skp_tables_skp_item_hierarchy where CHANNEL_CODE='Import'"},
                'USSAMS': {0: 'select * from spine_poc.us_wc_skp_tables_skp_item_hierarchy'}}

param_input_map = [-1632138001, 'ParamaterDTO [call="_SYS_BIC"."skp-bcbs-stage-hpc::SP_SPINE_REDESIGN_HEADROOM_MAP"  param= [11833, 11932, 0, 0, 1=1 and acctg_dept_desc in ("MEAT & SEAFOOD") and dept_catg_grp_desc in ("CHICKEN") and vendor_name in ("SANDERSON FARMS INC") and acctg_dept_nbr in ("93") and dept_catg_grp_nbr in ("1385") and vendor_nbr in ("332767"), COC, -1, -1, -1, -1, 1, 1, 1, 1, US]result location = null']
param_input_coo = [84962195, 'ParamaterDTO [call="_SYS_BIC"."skp-bcbs-stage-hpc::SP_SPINE_REDESIGN_HEADROOM_COO"              param= [11833, 11932, 0, 1=1 , 0, SALES, -1, -1, -1, -1, 1, 1, 1, 1, US]result location = null']
invalid_parms = []
invalid_hsk = []
hsk_string = ''

hive_table_cache = ['us_coc','ussams_coc','us_wm_tables_cvm_supp_diversity','us_wm_tables_vendor_cvm_supp','us_wm_skp_tables_skp_item_hierarchy','us_wc_skp_tables_skp_bu_dmn_item_wkly_summ']

HEADROOM_GEOSPATIAL = """SELECT MDS_FAM_ID,
                        SKP_BU_DOMAIN_CD,
                        SUM(SALES_AMT_TY) AS SALES_AMT_TY,
                        SUM(RECEIPTS_AMT_TY) AS RECEIPTS_AMT_TY,
                        SUM(FOB_W_DA_TY) AS FOB_W_DA_TY,
                        SUM(SALES_AMT_LY) AS SALES_AMT_LY,
                        SUM(RECEIPTS_AMT_LY) AS RECEIPTS_AMT_LY,
                        SUM(FOB_W_DA_LY) AS FOB_W_DA_LY,
                        SUM(WKLY_SALES_STORE_CNT) AS WKLY_SALES_STORE_CNT,
                        SUM(WKLY_SALES_QTY) AS WKLY_SALES_QTY,
                        SUM(FOB_STORE_COST_TY) AS STORE_COST_TY,
                        SUM(FOB_STORE_COST_LY) AS STORE_COST_LY
                        FROM
                            (SELECT SKP_BU_DOMAIN_CD,
                            MDS_FAM_ID,
                            WKLY_SALES_AMT AS SALES_AMT_TY,
                            WKLY_NET_SHIP_COST_AMT AS RECEIPTS_AMT_TY,
                            FOB_WITH_DA_ALLOWANCE_AMT AS FOB_W_DA_TY,
                            NULL AS SALES_AMT_LY,
                            NULL AS RECEIPTS_AMT_LY,
                            NULL AS FOB_W_DA_LY,
                            WKLY_SALES_STORE_CNT,
                            WKLY_SALES_QTY,
                            FOB_STORE_COST_AMT AS FOB_STORE_COST_TY,
                            NULL AS FOB_STORE_COST_LY
                            FROM SPINE_POC.US_WM_SKP_TABLES_SKP_BU_DMN_ITEM_WKLY_SUMM
                            WHERE WM_YR_WK BETWEEN {0} AND {1}

                            UNION ALL

                            SELECT SKP_BU_DOMAIN_CD,
                            MDS_FAM_ID,
                            NULL,NULL,NULL,
                            WKLY_SALES_AMT AS SALES_AMT_LY,
                            WKLY_NET_SHIP_COST_AMT AS RECEIPTS_AMT_LY,
                            FOB_WITH_DA_ALLOWANCE_AMT AS FOB_W_DA_LY,
                            NULL,NULL,NULL,
                            FOB_STORE_COST_AMT AS FOB_STORE_COST_LY
                            FROM SPINE_POC.US_WM_SKP_TABLES_SKP_BU_DMN_ITEM_WKLY_SUMM
                            WHERE WM_YR_WK BETWEEN {2} AND {3} ) x
                        GROUP BY MDS_FAM_ID,SKP_BU_DOMAIN_CD"""

SAMS_HEADROOM_GEOSPATIAL = """SELECT MDS_FAM_ID,
                            SKP_BU_DOMAIN_CD,
                            SUM(SALES_AMT_TY) AS SALES_AMT_TY,
                            SUM(RECEIPTS_AMT_TY) AS RECEIPTS_AMT_TY,
                            SUM(FOB_W_DA_TY) AS FOB_W_DA_TY,
                            SUM(SALES_AMT_LY) AS SALES_AMT_LY,
                            SUM(RECEIPTS_AMT_LY) AS RECEIPTS_AMT_LY,
                            SUM(FOB_W_DA_LY) AS FOB_W_DA_LY,
                            SUM(WKLY_SALES_STORE_CNT) AS WKLY_SALES_STORE_CNT,
                            SUM(WKLY_SALES_QTY) AS WKLY_SALES_QTY,
                            SUM(FOB_STORE_COST_TY) AS STORE_COST_TY,
                            SUM(FOB_STORE_COST_LY) AS STORE_COST_LY
                            FROM
                            	(SELECT SKP_BU_DOMAIN_CD,
                            	MDS_FAM_ID,
                            	WKLY_SALES_AMT AS SALES_AMT_TY,
                            	WKLY_NET_SHIP_COST_AMT AS RECEIPTS_AMT_TY,
                            	FOB_WITH_DA_ALLOWANCE_AMT AS FOB_W_DA_TY,
                            	NULL AS SALES_AMT_LY,
                            	NULL AS RECEIPTS_AMT_LY,
                            	NULL AS FOB_W_DA_LY,
                            	WKLY_SALES_STORE_CNT,
                            	WKLY_SALES_QTY,
                            	FOB_STORE_COST_AMT AS FOB_STORE_COST_TY,
                            	NULL AS FOB_STORE_COST_LY
                            	FROM spine_poc.us_wc_skp_tables_skp_bu_dmn_item_wkly_summ
                            	WHERE WM_YR_WK BETWEEN {0} AND {1}

                            	UNION ALL

                            	SELECT SKP_BU_DOMAIN_CD,
                            	MDS_FAM_ID,
                            	NULL,NULL,NULL,
                            	WKLY_SALES_AMT AS SALES_AMT_LY,
                            	WKLY_NET_SHIP_COST_AMT AS RECEIPTS_AMT_LY,
                            	FOB_WITH_DA_ALLOWANCE_AMT AS FOB_W_DA_LY,
                            	NULL,NULL,NULL,
                            	FOB_STORE_COST_AMT AS FOB_STORE_COST_LY
                            	FROM spine_poc.us_wc_skp_tables_skp_bu_dmn_item_wkly_summ
                            	WHERE WM_YR_WK BETWEEN {2} AND {3} )
                            GROUP BY MDS_FAM_ID,SKP_BU_DOMAIN_CD"""
