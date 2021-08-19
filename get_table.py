import json
import argparse
# 先将数据库分出来
# 每个数据库一个表
# 表名可有可无
# 无表名时统一用table
# 重命名成spider的格式
# 大小写转化

def transfer(table):
    # transfer one table
    re=dict()
    re["column_names"]=[]
    re["column_names"].append([-1,"*"])
    for column in table["header"]:
        re["column_names"].append([0,column.lower()])
    re["column_names_original"]=[]
    re["column_names_original"].append([-1,"*"])
    for i in range(len(table["header"])):
        re["column_names_original"].append([0,"col{}".format(i)])
    re["column_types"]=["text"]
    for member in table["types"]:
        if member=="text":
            re["column_types"].append(member)
        else:
            re["column_types"].append("number")

    re["db_id"]=table["id"]
    re["db_id"]=re["db_id"].replace("-","_")
    re["foreign_keys"]=[]
    re["primary_keys"]=[]
    if "page_title" in table:
        re["table_names"]=[table["page_title"].lower()]
    else:
        re["table_names"]=["table"]
    re["table_names_original"]=["table_"+re["db_id"]]
    return re

    
if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--wikipath")
    parser.add_argument("--target")
    # wiki_path="E:/lab/wikisql/WikiSQL-master/WikiSQL-master/data/data/dev.tables.jsonl"
    args=parser.parse_args()
    wiki_path=args.wikipath
    target_path=args.target
    with open(wiki_path) as f:
        table_text_lis=f.readlines()

    table = [transfer(json.loads(member)) for member in table_text_lis]
    table_json=json.dumps(table,indent=3)
    with open(target_path,"w") as f:
        f.write(table_json)
