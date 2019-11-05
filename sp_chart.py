__author__ = 'vn50fti'

from src.main.utility import generic_functions
from  src.main.constants import constants

class headroom_chart():
    def __init__(self, hive_input_list):
        self.START_WEEK = int(str.strip(hive_input_list[0]))
        self.END_WEEK = int(str.strip(hive_input_list[1]))
        self.STORE_LIMIT = int(str.strip(hive_input_list[2]))
        self.WHERE_COND = hive_input_list[3]
        self.SLOW_MOV = int(str.strip(hive_input_list[4]))
        self.TOGGLE = hive_input_list[5]
        self.GENDER_CD = hive_input_list[6]
        self.ETHNICITY_CD = hive_input_list[7]
        self.VETERAN_CD = hive_input_list[8]
        self.DISABLE_CD = hive_input_list[9]
        self.INCLUDE_US = int(str.strip(hive_input_list[10]))
        self.INCLUDE_DOTCOM = int(str.strip(hive_input_list[11]))
        self.INCLUDE_STATES = int(str.strip(hive_input_list[12]))
        self.HIERARCHY_FLAG = int(str.strip(hive_input_list[13]))
        self.MARKET = hive_input_list[14]

    def frameQuery(self):

        AT_TABLE_CHART = generic_functions.getAtTable(self.MARKET,self.HIERARCHY_FLAG)

        if self.INCLUDE_US == 0 and self.INCLUDE_DOTCOM == 0 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD = 3 '
        if self.INCLUDE_US == 0 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 0:
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD = 2 '
        if self.INCLUDE_US == 0 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD IN (2,3) '
        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 0 and self.INCLUDE_STATES == 0:
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD = 1 '
        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 0 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD IN (1,3)  '
        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 0:
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD IN (1,2) '
        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD IN (1,2,3)'

        if self.MARKET=='US':
            FACT_VIEW = """ (SELECT SKP_BU_DOMAIN_CD, WM_YR_WK, MDS_FAM_ID, SUM(WKLY_SALES_STORE_CNT) AS WKLY_SALES_STORE_CNT, SUM(WKLY_SALES_AMT) AS WKLY_SALES_AMT, SUM(WKLY_SALES_QTY) AS WKLY_SALES_QTY, SUM(WKLY_NET_SHIP_COST_AMT) AS WKLY_NET_SHIP_COST_AMT FROM spine_poc.us_wm_skp_tables_skp_bu_dmn_item_wkly_summ WHERE WM_YR_WK BETWEEN {0} and {1} GROUP BY SKP_BU_DOMAIN_CD, WM_YR_WK, MDS_FAM_ID ) M1""".format(self.START_WEEK,self.END_WEEK)
            FACT_VIEW_1= """ (SELECT SKP_BU_DOMAIN_CD,WM_YR_WK, MDS_FAM_ID,SUM(WKLY_SALES_STORE_CNT) AS WKLY_SALES_STORE_CNT, SUM(WKLY_SALES_AMT) AS WKLY_SALES_AMT, SUM(WKLY_SALES_QTY) AS WKLY_SALES_QTY, SUM(WKLY_NET_SHIP_COST_AMT) AS WKLY_NET_SHIP_COST_AMT FROM spine_poc.us_wm_skp_tables_skp_bu_dmn_item_wkly_summ WHERE WM_YR_WK BETWEEN {0} and {0} GROUP BY SKP_BU_DOMAIN_CD, WM_YR_WK, MDS_FAM_ID ) N1""".format(self.END_WEEK)
        elif self.MARKET=='USSAMS':
            INCLUDE_QUERY =  'SKP_BU_DOMAIN_CD = 4 '
            FACT_VIEW  = """ (SELECT SKP_BU_DOMAIN_CD, WM_YR_WK, MDS_FAM_ID, SUM(WKLY_SALES_STORE_CNT) AS WKLY_SALES_STORE_CNT, SUM(WKLY_SALES_AMT) AS WKLY_SALES_AMT, SUM(WKLY_SALES_QTY) AS WKLY_SALES_QTY, SUM(WKLY_NET_SHIP_COST_AMT) AS WKLY_NET_SHIP_COST_AMT FROM spine_poc.us_wc_skp_tables_skp_bu_dmn_item_wkly_summ  WHERE WM_YR_WK BETWEEN {0} and {1} GROUP BY SKP_BU_DOMAIN_CD, WM_YR_WK, MDS_FAM_ID ) M1""".format(self.START_WEEK, self.END_WEEK)
            FACT_VIEW_1 = """ (SELECT SKP_BU_DOMAIN_CD,WM_YR_WK, MDS_FAM_ID, SUM(WKLY_SALES_STORE_CNT) AS WKLY_SALES_STORE_CNT, SUM(WKLY_SALES_AMT) AS WKLY_SALES_AMT, SUM(WKLY_SALES_QTY) AS WKLY_SALES_QTY, SUM(WKLY_NET_SHIP_COST_AMT) AS WKLY_NET_SHIP_COST_AMT FROM spine_poc.us_wc_skp_tables_skp_bu_dmn_item_wkly_summ  WHERE WM_YR_WK BETWEEN {0} and {0} GROUP BY SKP_BU_DOMAIN_CD, WM_YR_WK, MDS_FAM_ID ) N1""".format(self.END_WEEK)

        if self.STORE_LIMIT>0 and self.SLOW_MOV==0:
            TABLE_NAME =' and MDS_FAM_ID IN (SELECT MDS_FAM_ID FROM {0} WHERE WKLY_SALES_STORE_CNT >{1} GROUP BY MDS_FAM_ID)'.format(FACT_VIEW_1, self.STORE_LIMIT)
            TEMP_TABLE_CONDITION = ''
        elif self.STORE_LIMIT==0 and self.SLOW_MOV==1:
            TABLE_NAME = ''
            TEMP_TABLE_CONDITION = ' HAVING SUM(WKLY_SALES_QTY)/(case when SUM(WKLY_SALES_STORE_CNT) =0 then NULL else SUM(WKLY_SALES_STORE_CNT) end) >0.5 '
        elif self.STORE_LIMIT>0 and self.SLOW_MOV==1:
            TABLE_NAME = ' and MDS_FAM_ID IN (SELECT MDS_FAM_ID FROM {0} WHERE WKLY_SALES_STORE_CNT >{1} GROUP BY MDS_FAM_ID)'.format(FACT_VIEW_1, self.STORE_LIMIT)
            TEMP_TABLE_CONDITION =' HAVING SUM(WKLY_SALES_QTY)/(case when SUM(WKLY_SALES_STORE_CNT) =0 then NULL else SUM(WKLY_SALES_STORE_CNT) end) >0.5 '
        elif self.STORE_LIMIT==0 and self.SLOW_MOV==0:
            TABLE_NAME = ''
            TEMP_TABLE_CONDITION =' '

        if self.TOGGLE=='SALES':
            SELECT_SALES_RECEIPT ='WKLY_SALES_AMT'
            COLUMN_SALES_RECEIPT ='SALES'
        else:
            SELECT_SALES_RECEIPT ='WKLY_NET_SHIP_COST_AMT'
            COLUMN_SALES_RECEIPT ='RECEIPTS'

        if self.ETHNICITY_CD=='-1':
            ETHNICITY= '1=1'
        else:
            ETHNICITY = 'ETHNICITY_CODE IN ({0})'.format(self.ETHNICITY_CD)

        if self.VETERAN_CD=='-1':
            VETERAN= '1=1'
        else:
            VETERAN= "VETERAN_IND IN ('{0}')".format(self.VETERAN_CD)

        if self.DISABLE_CD=='-1':
            DISABLED= '1=1'
        else:
            DISABLED= "DISABLE_IND IN ('{0}')".format(self.DISABLE_CD)

        if self.GENDER_CD=='-1':
            GENDER= '1=1'
        else:
            GENDER= "GENDER_CODE IN ('{}')".format(self.GENDER_CD)

        if ((self.GENDER_CD != '-1') or (self.ETHNICITY_CD != '-1') or (self.VETERAN_CD != '-1') or (self.DISABLE_CD != '-1')):
            DIVERSITY_TABLE =" INNER JOIN (SELECT VENDOR_NBR,GENDER_CODE, ETHNICITY_CODE,VETERAN_IND,DISABLE_IND FROM  (SELECT VENDOR_NBR,GENDER_CODE, ETHNICITY_CODE,VETERAN_IND,DISABLE_IND FROM spine_poc.US_WM_TABLES_CVM_SUPP_DIVERSITY AS A RIGHT JOIN spine_poc.US_WM_TABLES_VENDOR_CVM_SUPP AS B ON B.CVM_SUPPLIER_NBR=A.CVM_SUPPLIER_NBR) C WHERE {0} AND {1} AND {2} AND {3}) AS V ON V.VENDOR_NBR=A.VENDOR_NBR ".format(GENDER,ETHNICITY,VETERAN,DISABLED)
        else:
            DIVERSITY_TABLE=' '

        if self.MARKET=='US':
            QUERY ="""SELECT (case when ACCTG_DEPT_NBR is null then -123454321 else ACCTG_DEPT_NBR end) ACCTG_DEPT_NBR,
                (case when ACCTG_DEPT_DESC is null then 'WALMART_SPINE_REDESIGN' else ACCTG_DEPT_DESC end) ACCTG_DEPT_DESC,
                (case when DEPT_CATEGORY_NBR is null then -123454321 else DEPT_CATEGORY_NBR end) DEPT_CATEGORY_NBR,
                                (case when DEPT_CATEGORY_DESC is null then 'WALMART_SPINE_REDESIGN' else DEPT_CATEGORY_DESC end) DEPT_CATEGORY_DESC,
                (case when CHANNEL_CODE is null then 'WALMART_SPINE_REDESIGN' else CHANNEL_CODE end) CHANNEL_CODE,
                 (case when  BRAND_CODE is null then 'WALMART_SPINE_REDESIGN' else  BRAND_CODE end) BRAND_CODE,
                 (case when {0} is null then cast(-123454321.99 as decimal(15,2)) else {0} end)  AS {0}
                FROM (SELECT A.ACCTG_DEPT_NBR,A.ACCTG_DEPT_DESC,DEPT_CATEGORY_NBR,DEPT_CATEGORY_DESC, A.CHANNEL_CODE,A.BRAND_CODE,SUM(COALESCE({0},0)) AS {0}
                                FROM  (SELECT * FROM ({1}) a WHERE {2}) A {3} INNER JOIN (SELECT MDS_FAM_ID,SUM(COALESCE({4},0)) AS {0} FROM {5} WHERE {6} {7} GROUP BY MDS_FAM_ID {8} ) AS B
                                ON B.MDS_FAM_ID=A.MDS_FAM_ID GROUP BY  A.ACCTG_DEPT_NBR,A.ACCTG_DEPT_DESC,DEPT_CATEGORY_NBR,DEPT_CATEGORY_DESC, A.CHANNEL_CODE,A.BRAND_CODE
                                ORDER BY  A.ACCTG_DEPT_NBR,A.ACCTG_DEPT_DESC,DEPT_CATEGORY_NBR,DEPT_CATEGORY_DESC,A.CHANNEL_CODE,A.BRAND_CODE) x""".format(COLUMN_SALES_RECEIPT,AT_TABLE_CHART,self.WHERE_COND,DIVERSITY_TABLE,SELECT_SALES_RECEIPT,FACT_VIEW,INCLUDE_QUERY,TABLE_NAME,TEMP_TABLE_CONDITION)
        elif self.MARKET=='USSAMS':
            QUERY ="""SELECT (case when ACCTG_DEPT_NBR is null then -123454321 else ACCTG_DEPT_NBR end) ACCTG_DEPT_NBR,
                (case when ACCTG_DEPT_DESC is null then 'WALMART_SPINE_REDESIGN' else ACCTG_DEPT_DESC end) ACCTG_DEPT_DESC,
                (case when CHANNEL_CODE is null then 'WALMART_SPINE_REDESIGN' else CHANNEL_CODE end) CHANNEL_CODE,
                 (case when  BRAND_CODE is null then 'WALMART_SPINE_REDESIGN' else  BRAND_CODE end) BRAND_CODE,
                 (case when {0} is null then cast(-123454321.99 as decimal(15,2)) else {0} end)  AS {0}
                FROM (SELECT A.ACCTG_DEPT_NBR,A.ACCTG_DEPT_DESC, A.CHANNEL_CODE,A.BRAND_CODE,SUM(COALESCE({0},0)) AS {0}
                                FROM  (SELECT * FROM ({1}) a WHERE {2}) A {3} INNER JOIN (SELECT MDS_FAM_ID,SUM(COALESCE({4},0)) AS {0} FROM {5} WHERE {6} {7} GROUP BY MDS_FAM_ID {8} ) AS B
                                ON B.MDS_FAM_ID=A.MDS_FAM_ID GROUP BY  A.ACCTG_DEPT_NBR,A.ACCTG_DEPT_DESC, A.CHANNEL_CODE,A.BRAND_CODE
                                ORDER BY  A.ACCTG_DEPT_NBR,A.ACCTG_DEPT_DESC,A.CHANNEL_CODE,A.BRAND_CODE) x """.format(COLUMN_SALES_RECEIPT,AT_TABLE_CHART,self.WHERE_COND,DIVERSITY_TABLE,SELECT_SALES_RECEIPT,FACT_VIEW,INCLUDE_QUERY,TABLE_NAME,TEMP_TABLE_CONDITION)

        return QUERY

if __name__ == '__main__':
        chart_run = headroom_chart([])
        chart_run.frameQuery()
