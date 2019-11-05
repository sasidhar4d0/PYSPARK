__author__ = 'vn50fti'

from src.main.utility import generic_functions
from  src.main.constants import constants

class headroom_map():
    """ Class created for headroom map stored procedure"""

    def __init__(self,hive_input_list):
        """ Constructor for headroom_map class"""

        self.START_WEEK = int(str.strip(hive_input_list[0]))
        self.END_WEEK = int(str.strip(hive_input_list[1]))
        self.STORE_LIMIT = int(str.strip(hive_input_list[2]))
        self.SLOW_MOV = int(str.strip(hive_input_list[3]))
        self.WHERE_CONDITION = hive_input_list[4]
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
        self.frameQuery()


    def frameQuery(self):
        """ Function to initialize variables to frame the hive query """

        HEADROOM_GEOSPATIAL_1 = ''
        HEADROOM_GEOSPATIAL = ''
        INCLUDE_QUERY = ''
        TABLE_NAME = ''
        TEMP_TABLE_CONDITION = ''

        AT_TABLE = generic_functions.getAtTable(self.MARKET,self.HIERARCHY_FLAG)

        if self.INCLUDE_US == 0 and self.INCLUDE_DOTCOM == 0 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD = 3'

        if self.INCLUDE_US == 0 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 0:
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD = 2'

        if self.INCLUDE_US == 0 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD IN (2,3)'

        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 0 and self.INCLUDE_STATES == 0:
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD = 1'

        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 0 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD IN (1,3)'

        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 0:
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD IN (1,2)'

        if self.INCLUDE_US == 1 and self.INCLUDE_DOTCOM == 1 and self.INCLUDE_STATES == 1:
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD IN (1,2,3)'

        if self.MARKET == 'US':
            HEADROOM_GEOSPATIAL = constants.HEADROOM_GEOSPATIAL.format(self.START_WEEK,self.END_WEEK,self.START_WEEK-100,self.END_WEEK-100)
            HEADROOM_GEOSPATIAL_1 = constants.HEADROOM_GEOSPATIAL.format(self.END_WEEK,self.END_WEEK,self.END_WEEK-100,self.END_WEEK-100)

        if self.MARKET == 'USSAMS':
            INCLUDE_QUERY = 'SKP_BU_DOMAIN_CD = 4'
            HEADROOM_GEOSPATIAL = constants.SAMS_HEADROOM_GEOSPATIAL.format(self.START_WEEK,self.END_WEEK,self.START_WEEK-100,self.END_WEEK-100)
            HEADROOM_GEOSPATIAL_1 = constants.SAMS_HEADROOM_GEOSPATIAL.format(self.END_WEEK,self.END_WEEK,self.END_WEEK-100,self.END_WEEK-100)

        if self.STORE_LIMIT > 0 and self.SLOW_MOV == 0:
            TABLE_NAME = ''' AND MDS_FAM_ID IN (SELECT MDS_FAM_ID
                           FROM {0}
                            WHERE WKLY_SALES_STORE_CNT > {1}
                           GROUP BY MDS_FAM_ID)'''.format(HEADROOM_GEOSPATIAL_1,self.STORE_LIMIT)
            TEMP_TABLE_CONDITION = ''

        if self.STORE_LIMIT == 0 and self.SLOW_MOV == 1:
            TABLE_NAME = ''
            TEMP_TABLE_CONDITION = 'HAVING SUM(WKLY_SALES_QTY)/(case when SUM(WKLY_SALES_STORE_CNT) =0 then NULL else SUM(WKLY_SALES_STORE_CNT) END) >0.5 '

        if self.STORE_LIMIT > 0 and self.SLOW_MOV == 1 :
            TABLE_NAME = ''' AND MDS_FAM_ID IN (SELECT MDS_FAM_ID
                           FROM {0}
                            WHERE WKLY_SALES_STORE_CNT >{1}
                           GROUP BY MDS_FAM_ID)'''.format(HEADROOM_GEOSPATIAL_1,self.STORE_LIMIT)
            TEMP_TABLE_CONDITION = ' HAVING SUM(WKLY_SALES_QTY)/(case when SUM(WKLY_SALES_STORE_CNT) =0 then NULL else SUM(WKLY_SALES_STORE_CNT) END) >0.5 '

        if self.STORE_LIMIT == 0 and self.SLOW_MOV == 0:
            TABLE_NAME = ''
            TEMP_TABLE_CONDITION = ''

        if self.GENDER_CD == '-1':
            GENDER = '1=1'
        else:
            GENDER = "GENDER_CODE IN ('{0}')".format(self.GENDER_CD)

        if self.ETHNICITY_CD == '-1':
            ETHNICITY = '1=1'
        else:
            ETHNICITY = "ETHNICITY_CODE IN ('{0}')".format(self.ETHNICITY_CD)

        if self.VETERAN_CD == '-1':
            VETERAN = '1=1'
        else:
            VETERAN = "VETERAN_IND IN ('{0}')".format(self.VETERAN_CD)

        if self.DISABLE_CD == '-1':
            DISABLED = '1=1'
        else:
            DISABLED = "DISABLE_IND IN ('{0}')".format(self.DISABLE_CD)

        if self.GENDER_CD != '-1' or self.ETHNICITY_CD != '-1' or self.VETERAN_CD != '-1' or self.DISABLE_CD != '-1':
            DIVERSITY_TABLE = """ INNER JOIN (SELECT VENDOR_NBR,GENDER_CODE, ETHNICITY_CODE,VETERAN_IND,DISABLE_IND FROM
                                        (SELECT VENDOR_NBR,GENDER_CODE, ETHNICITY_CODE,VETERAN_IND,DISABLE_IND FROM spine_poc.us_wm_tables_cvm_supp_diversity AS A
                                                RIGHT JOIN
                                                spine_poc.us_wm_tables_vendor_cvm_supp AS B
                                                ON B.CVM_SUPPLIER_NBR=A.CVM_SUPPLIER_NBR)WHERE {0} AND {1} AND {2} AND {3}) AS V
                                                ON V.VENDOR_NBR=A.VENDOR_NBR """.format(GENDER,ETHNICITY,VETERAN,DISABLED)
        else:
            DIVERSITY_TABLE = ''

        if self.TOGGLE == 'COC'  and self.MARKET == 'US':
            COC_QUERY = """select * from spine_poc.us_coc"""

        if self.TOGGLE == 'COC' and self.MARKET == 'USSAMS':
            COC_QUERY = """select * from spine_poc.ussams_coc"""

        if self.TOGGLE == 'COO':
            QUERY = """SELECT (case when COUNTRY_CODE is null then 'WALMART_SPINE_REDESIGN' else COUNTRY_CODE end) COUNTRY_CODE,
                    (case when VENDOR_COUNT is null then -123454321 else VENDOR_COUNT end) AS VENDOR_COUNT,
                    (case when SALES_TY is null then cast(-123454321.99 as decimal(15,2)) else SALES_TY end) AS SALES_TY,
                    (case when SALES_LY is null then cast(-123454321.99 as decimal(15,2)) else SALES_LY end) AS SALES_LY,
                                        (case when PVT_SALES_TY is null then cast(-123454321.99 as decimal(15,2)) else PVT_SALES_TY end) AS PVT_SALES_TY,
                                        (case when PVT_SALES_LY is null then cast(-123454321.99 as decimal(15,2)) else PVT_SALES_LY end) AS PVT_SALES_LY,
                                        (case when RECEIPTS_TY is null then cast(-123454321.99 as decimal(15,2)) else RECEIPTS_TY end) AS RECEIPTS_TY,
                                        (case when RECEIPTS_LY is null then cast(-123454321.99 as decimal(15,2)) else RECEIPTS_LY end) AS RECEIPTS_LY,
                                        (case when FOB_W_DA_TY is null then cast(-123454321.99 as decimal(15,2)) else FOB_W_DA_TY end) AS FOB_W_DA_TY,
                                        (case when FOB_W_DA_LY is null then cast(-123454321.99 as decimal(15,2)) else FOB_W_DA_LY end) AS FOB_W_DA_LY,
                                        (case when STORE_COST_TY is null then cast(-123454321.99 as decimal(15,2)) else STORE_COST_TY end) AS STORE_COST_TY,
                                        (case when STORE_COST_LY is null then cast(-123454321.99 as decimal(15,2)) else STORE_COST_LY end) AS STORE_COST_LY
                                        FROM (SELECT COUNTRY_CODE, COUNT(DISTINCT A.VENDOR_NBR) AS VENDOR_COUNT,SUM(SALES_TY) AS SALES_TY, SUM(SALES_LY) AS SALES_LY,
                                        SUM(CASE WHEN BRAND_CODE='P' THEN SALES_TY ELSE NULL END) AS PVT_SALES_TY,SUM(CASE WHEN BRAND_CODE='P' THEN SALES_LY ELSE NULL END) AS PVT_SALES_LY,
                                        SUM(RECEIPTS_TY) AS RECEIPTS_TY, SUM(RECEIPTS_LY) AS RECEIPTS_LY,SUM(FOB_W_DA_TY) AS FOB_W_DA_TY, SUM(FOB_W_DA_LY) AS FOB_W_DA_LY,
                                        SUM(STORE_COST_TY) AS STORE_COST_TY, SUM(STORE_COST_LY) AS STORE_COST_LY FROM
                                        (SELECT MDS_FAM_ID, SUM(SALES_AMT_TY) AS SALES_TY, SUM(RECEIPTS_AMT_TY) AS RECEIPTS_TY, SUM(SALES_AMT_LY) AS SALES_LY,
                                        SUM(RECEIPTS_AMT_LY) AS RECEIPTS_LY, SUM(FOB_W_DA_TY) AS FOB_W_DA_TY, SUM(FOB_W_DA_LY) AS FOB_W_DA_LY,
                                        SUM(STORE_COST_TY) AS STORE_COST_TY, SUM(STORE_COST_LY) AS STORE_COST_LY
                                        FROM ( {0} ) y WHERE {1} {2} GROUP BY MDS_FAM_ID {3}) B INNER JOIN ({4}) A ON A.MDS_FAM_ID=B.MDS_FAM_ID {5} WHERE {6} GROUP BY COUNTRY_CODE
                                        ORDER BY COUNTRY_CODE) x""".format(HEADROOM_GEOSPATIAL,INCLUDE_QUERY,TABLE_NAME,TEMP_TABLE_CONDITION,AT_TABLE,DIVERSITY_TABLE,self.WHERE_CONDITION)

        if self.TOGGLE == 'COC':
            QUERY = """SELECT (case when CREDIT_OFFICE_ID is null then -123454321 else CREDIT_OFFICE_ID end) CREDIT_OFFICE_ID,
                                        (case when VENDOR_COUNT is null then -123454321 else VENDOR_COUNT end) AS VENDOR_COUNT,
                                        (case when SALES_TY is null then cast(-123454321.99 as decimal(15,2)) else SALES_TY end) AS SALES_TY,
                                        (case when SALES_LY is null then cast(-123454321.99 as decimal(15,2)) else SALES_LY end) AS SALES_LY,
                                        (case when PVT_SALES_TY is null then cast(-123454321.99 as decimal(15,2)) else PVT_SALES_TY end) AS PVT_SALES_TY,
                                        (case when PVT_SALES_LY is null then cast(-123454321.99 as decimal(15,2)) else PVT_SALES_LY end) AS PVT_SALES_LY,
                                        (case when RECEIPTS_TY is null then cast(-123454321.99 as decimal(15,2)) else RECEIPTS_TY end) AS RECEIPTS_TY,
                                        (case when RECEIPTS_LY is null then cast(-123454321.99 as decimal(15,2)) else RECEIPTS_LY end) AS RECEIPTS_LY,
                                        (case when FOB_W_DA_TY is null then cast(-123454321.99 as decimal(15,2)) else FOB_W_DA_TY end) AS FOB_W_DA_TY,
                                        (case when FOB_W_DA_LY is null then cast(-123454321.99 as decimal(15,2)) else FOB_W_DA_LY end) AS FOB_W_DA_LY,
                                        (case when STORE_COST_TY is null then cast(-123454321.99 as decimal(15,2)) else STORE_COST_TY end) AS STORE_COST_TY,
                                        (case when STORE_COST_LY is null then cast(-123454321.99 as decimal(15,2)) else STORE_COST_LY end) AS STORE_COST_LY,
                                        (case when CREDIT_OFFICE_NAME is null then 'WALMART_SPINE_REDESIGN' else CREDIT_OFFICE_NAME end) CREDIT_OFFICE_NAME,
                    (case when LONGITUDE is null then -123454321 else LONGITUDE end) LONGITUDE,
                                    (case when LATITUDE is null then -123454321 else LATITUDE end) LATITUDE
                                        FROM (SELECT D.*, CREDIT_OFFICE_NAME, LONGITUDE, LATITUDE FROM
                                        (SELECT CREDIT_OFFICE_ID, COUNT(DISTINCT VENDOR_NBR) AS VENDOR_COUNT,SUM(SALES_TY) AS SALES_TY, SUM(SALES_LY) AS SALES_LY,SUM(PVT_SALES_TY) AS PVT_SALES_TY,
                                        SUM(PVT_SALES_LY) AS PVT_SALES_LY, SUM(RECEIPTS_TY) AS RECEIPTS_TY, SUM(RECEIPTS_LY) AS RECEIPTS_LY, SUM(FOB_W_DA_TY) AS FOB_W_DA_TY, SUM(FOB_W_DA_LY) AS FOB_W_DA_LY,
                                        SUM(STORE_COST_TY) AS STORE_COST_TY, SUM(STORE_COST_LY) AS STORE_COST_LY FROM
                                        (SELECT A.ITEM_NBR,CREDIT_OFFICE_ID,  A.VENDOR_NBR,SUM(SALES_TY) AS SALES_TY, SUM(SALES_LY) AS SALES_LY,
                                        SUM(CASE WHEN BRAND_CODE='P' THEN SALES_TY ELSE NULL END) AS PVT_SALES_TY, SUM(CASE WHEN BRAND_CODE='P' THEN SALES_LY ELSE NULL END) AS PVT_SALES_LY,
                                        SUM(RECEIPTS_TY) AS RECEIPTS_TY, SUM(RECEIPTS_LY) AS RECEIPTS_LY, SUM(FOB_W_DA_TY) AS FOB_W_DA_TY, SUM(FOB_W_DA_LY) AS FOB_W_DA_LY,
                                        SUM(STORE_COST_TY) AS STORE_COST_TY, SUM(STORE_COST_LY) AS STORE_COST_LY FROM
                                        (SELECT MDS_FAM_ID, SUM(SALES_AMT_TY) AS SALES_TY, SUM(RECEIPTS_AMT_TY) AS RECEIPTS_TY, SUM(SALES_AMT_LY) AS SALES_LY, SUM(RECEIPTS_AMT_LY) AS RECEIPTS_LY,
                                        SUM(FOB_W_DA_TY) AS FOB_W_DA_TY, SUM(FOB_W_DA_LY) AS FOB_W_DA_LY, SUM(STORE_COST_TY) AS STORE_COST_TY, SUM(STORE_COST_LY) AS STORE_COST_LY
                                        FROM ({0}) y WHERE {1} {2} GROUP BY MDS_FAM_ID {3}) C INNER JOIN ({4}) A ON C.MDS_FAM_ID=A.MDS_FAM_ID {5} WHERE {6}
                                        GROUP BY A.ITEM_NBR, CREDIT_OFFICE_ID, A.VENDOR_NBR) Z GROUP BY CREDIT_OFFICE_ID) D INNER JOIN ({7}) E
                                        ON D.CREDIT_OFFICE_ID= E.CREDIT_OFFICE_ID) x""".format(HEADROOM_GEOSPATIAL,INCLUDE_QUERY,TABLE_NAME,TEMP_TABLE_CONDITION,AT_TABLE,DIVERSITY_TABLE,self.WHERE_CONDITION,COC_QUERY)

        return QUERY


if __name__ == '__main__':
    y = [-1632138001, 'ParamaterDTO [call="_SYS_BIC"."skp-bcbs-stage-hpc::SP_SPINE_REDESIGN_HEADROOM_MAP"  param= [11833, 11932, 0, 0, 1=1 and acctg_dept_desc in ("MEAT & SEAFOOD") and dept_catg_grp_desc in ("CHICKEN") and vendor_name in ("SANDERSON FARMS INC") and acctg_dept_nbr in ("93") and dept_catg_grp_nbr in ("1385") and vendor_nbr in ("332767"), COO, -1, -1, -1, -1, 1, 1, 1, 1, US]result location = null']
    #print(generic_functions.generateProcedureParams(y))
    x = headroom_map(generic_functions.generateProcedureParams(y)[0][1])
    #print(x.MARKET)
    print(x.frameQuery())
