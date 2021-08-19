import json
import os
import traceback
import re
import sys
import json
import sqlite3
import sqlparse
import random
from os import listdir, makedirs
from collections import OrderedDict
from nltk import word_tokenize, tokenize
from os.path import isfile, isdir, join, split, exists, splitext
sys.path.append(os.getcwd())
from spider.process_sql import get_sql
import argparse
class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """
    def __init__(self, schema, table):
        self._schema = schema
        self._table = table
        self._idMap = self._map(self._schema, self._table)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema, table):
        column_names_original = table['column_names_original']
        table_names_original = table['table_names_original']
        #print 'column_names_original: ', column_names_original
        #print 'table_names_original: ', table_names_original
        for i, (tab_id, col) in enumerate(column_names_original):
            if tab_id == -1:
                idMap = {'*': i}
            else:
                key = table_names_original[tab_id].lower()
                val = col.lower()
                idMap[key + "." + val] = i

        for i, tab in enumerate(table_names_original):
            key = tab.lower()
            idMap[key] = i

        return idMap


def get_schemas_from_json(fpath):
    with open(fpath) as f:
        data = json.load(f)
    db_names = [db['db_id'] for db in data]

    tables = {}
    schemas = {}
    for db in data:
        db_id = db['db_id']
        schema = {} #{'table': [col.lower, ..., ]} * -> __all__
        column_names_original = db['column_names_original']
        table_names_original = db['table_names_original']
        tables[db_id] = {'column_names_original': column_names_original, 'table_names_original': table_names_original}
        for i, tabn in enumerate(table_names_original):
            table = str(tabn.lower())
            cols = [str(col.lower()) for td, col in column_names_original if td == i]
            schema[table] = cols
        schemas[db_id] = schema

    return schemas, db_names, tables

agg_ops = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
cond_ops = ['=', '>', '<', 'OP']
syms = ['SELECT', 'WHERE', 'AND', 'COL', 'TABLE', 'CAPTION', 'PAGE', 'SECTION', 'OP', 'COND', 'QUESTION', 'AGG', 'AGGOPS', 'CONDOPS']

def get_conds(conds,value):
    if len(conds)==0:
        return ""
    re="WHERE "
    for i, member in enumerate(conds):
        if i!=0:
            re+='AND '
        re+="col{}".format(member[0])+" "+cond_ops[member[1]]
        if value:
            if type(member[2]==str):
                re+=" \'{}\'".format(member[2])+' '
            else:
                re+=" {}".format(member[2])
        else:
            re+=" value"
    return re
    
def to_sql(q,value):
    re="SELECT "    
    tid=q["table_id"]
    tname=tid.replace("-","_")
    tname="table_"+tname
    sel=q["sql"]["sel"]
    conds=q["sql"]["conds"]
    if q["sql"]["agg"]!=0:
        re+=agg_ops[q["sql"]["agg"]]+'('
        re+="col{}".format(sel)
        re+=')'
    else:
        re+="col{}".format(sel)
    re+=" FROM "+tname+' '
    re+=get_conds(conds,value)
    return re


if __name__ == '__main__':
    
    # sql = "SELECT name ,  country ,  age FROM singer ORDER BY age DESC"
    # db_id = "concert_singer"
    # table_file = "tables.json"
    # wiki_path="E:/lab/wikisql/WikiSQL-master/WikiSQL-master/data/data/dev.jsonl"
    # table_file = "spider/tem_table.json"
    parser=argparse.ArgumentParser()
    parser.add_argument("--wikipath")
    parser.add_argument("--tablepath")
    parser.add_argument("--target")
    args=parser.parse_args()
    wiki_path=args.wikipath
    table_file=args.tablepath
    target_path=args.target
    with open(wiki_path) as f:
        table_text_lis=f.readlines()

    re=[]
    for i,line in  enumerate(table_text_lis):
        if i>10:
            break
        member=json.loads(line)
        sql=to_sql(member,True)
        sql_no_value=to_sql(member,False).lower()
        new_member=dict()
        
        new_member["db_id"]=member["table_id"].replace("-","_")
        new_member["query"]=sql
        new_member["query_toks"]=word_tokenize(sql)
        new_member["query_toks_no_value"]=word_tokenize(sql_no_value)
        new_member["question"]=member["question"]
        new_member["question_toks"]=word_tokenize(member["question"])
        
        schemas, db_names, tables = get_schemas_from_json(table_file)
        db_id=new_member["db_id"]
        schema = schemas[db_id]
        table = tables[db_id]
        schema = Schema(schema, table)
        new_member["sql"] = get_sql(schema, sql)
        re.append(new_member)
    
    re=json.dumps(re,indent=3)
    with open(target_path,"w") as f:
        f.write(re)


    
    

    