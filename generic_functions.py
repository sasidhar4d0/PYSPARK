__author__ = 'vn50fti'

from src.main.constants import constants
import re
import copy

def getProcedureMappingTable(spark):
    """ Take Sparksession object as input
    hit and get the hive table data and convert into List[[List],[List]] output
    List[Hashcode_ID, Filter_Condition_TXT] """

    df = spark.sql("select distinct * from spine_poc.procedure_mapping where filter_condition_txt like '%SP_SPINE_REDESIGN_%'")
    #df = spark.sql("select distinct * from spine_poc.procedure_mapping where cast(filter_hashcode_id as string) in ('1004862259', '722769843', '-1685123165', '-557990716', '581699406', '1215660947', '1845841218', '1707307145', '-1742461989', '112544053', '-161083198')")
    return [[row.filter_hashcode_id, str(row.filter_condition_txt)] for row in df.select("filter_hashcode_id","filter_condition_txt").collect()]

def generateProcedureParams(hive_input):
    """ Take the input from getProcedureMappingTable function
    and return the List[Hashcode_ID, List[hive_sp_params]] output
    List[Hashcode_ID, List[15 params to run the procedure]] """
    params = []
    #return [[i[0], re.split(r',\s*(?![^()]*\))',str(i[1]).split('[')[2].split(']')[0]), str(i[1]).split('SP_SPINE_REDESIGN_')[1].split('"')[0]] for i in hive_input]
    for i in hive_input:
        if 'SP_SPINE_REDESIGN_HEADROOM_MAP' in i[1]:
            j = i[1].split('param= [')[1].split(']result')[0].split(',')
            op = ''
            for k in j[4:len(j)-10]:
                op += k+','
            params.append([i[0],[str(j[0]).strip(),str(j[1]).strip(),str(j[2]).strip(),str(j[3]).strip(),op.rstrip(',').strip(),str(j[-10]).strip(),str(j[-9]).strip(),str(j[-8]).strip(),str(j[-7]).strip(),str(j[-6]).strip(),str(j[-5]).strip(),str(j[-4]).strip(),str(j[-3]).strip(),str(j[-2]).strip(),str(j[-1]).strip()],str(i[1]).split('SP_SPINE_REDESIGN_')[1].split('"')[0]])
        else:
            j = i[1].split('param= [')[1].split(']result')[0].split(',')
            op = ''
            for k in j[3:len(j)-11]:
                op += k+','
            params.append([i[0],[str(j[0]).strip(),str(j[1]).strip(),str(j[2]).strip(),op.rstrip(',').strip(),str(j[-11]).strip(),str(j[-10]).strip(),str(j[-9]).strip(),str(j[-8]).strip(),str(j[-7]).strip(),str(j[-6]).strip(),str(j[-5]).strip(),str(j[-4]).strip(),str(j[-3]).strip(),str(j[-2]).strip(),str(j[-1]).strip()],str(i[1]).split('SP_SPINE_REDESIGN_')[1].split('"')[0]])
    return params


def getAtTable(market,hFlag):
    if market == 'USSAMS':
        return constants.at_table_map[market][0]
    elif market == 'US' and hFlag != 3:
        return constants.at_table_map[market][0]
    else:
        return constants.at_table_map[market][3]

def cache_table(spark,table):
    spark.sql("CACHE TABLE SPINE_POC."+table)

def executeHiveQuery(spark,query):
    """ Function that take spark object and query as input and return the output"""

    return spark.sql(query).toJSON().collect()

def validateParamCount(sp_params,spark):
    """ Function to validate the length of params - used only for testing"""
    for i in sp_params:
        if len(i[1]) != 15 and 'TABLE' not in str.upper(i[2]):
            constants.invalid_parms.append(i[1])
            constants.invalid_hsk.append(i[0])
            constants.hsk_string += str(i[0]) + " "
    spark.sql('create table if not exists spine_poc.test102 as select {0} as a,{1} as b,{2} as c,"{3}" as d from spine_poc.ussams_coc limit 1'.format(len(constants.invalid_parms),len(sp_params) - len(constants.invalid_parms),len(sp_params),constants.hsk_string))

def findComponent(second_element_in_hive_input):
    pass

def commonProcedureParams():
    pass

def mapProcedureParams():
    pass

def chartProcedureParams():
    pass

def cooProcedureParams():
    pass
