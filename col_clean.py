import os
import json
import argparse

parser = argparse.ArgumentParser(description='**Cleaning Raw Column Names**')
parser.add_argument ('path_to_json', help='DBT Model-Json Files Absolute Path')
args = parser.parse_args()


json_files = []
"""
Input Argument : DBT Model-Json Files Absolute Path
Output : DBT Model
Clean Rules:1. Remove _c at the end of the column for all datatypes
            2. For timestamp columns => adding _utc_ts as suffix
            3. For boolean columns => adding _flag as suffix
            4. Key-cln_col_name in json object will override above all rules.
"""
def clean_cols(listOfDict):
    schema_new = []
    for i in listOfDict:
        if 'cln_col_name' in i:
            schema_new.append(i['cln_col_name'])
        else:
            rawColC = i['raw_col_name'].lower().endswith('_c')
            if i['data_type'].lower() == 'boolean':
                if rawColC == True:
                    s = i['raw_col_name'][:-2] + '_flag'
                    schema_new.append(s)
                else:
                    schema_new.append(i['raw_col_name'] + '_flag')
            elif i['data_type'].lower() == 'timestamp':
                if rawColC == True:
                    s = i['raw_col_name'][:-2] + '_utc_ts'
                    schema_new.append(s)
                else:
                    schema_new.append(i['raw_col_name'] + '_utc_ts')
            else:
                if rawColC == True:
                    schema_new.append(i['raw_col_name'][:-2])
                else:
                    schema_new.append(i['raw_col_name'])
    return schema_new

def main():
    for root, dirs, files in os.walk(args.path_to_json):
        for f in files:
            if f.endswith('.json'):      #Check for .json extension 
                json_files.append(os.path.join(root, f))    #append full path to file
                index = f.find('.json')
                model_name = f[:index]
                # print(modelName)   
                for json_file in json_files:
                    schema_list = []
                    with open(json_file) as f:
                        # data=myfile.read()
                        # schemaList = []
                        for json_file in f:
                            schema_dict = json.loads(json_file)  
                            schema_list.append(schema_dict)
                            schema_clean = clean_cols(schema_list)
                            path = os.path.join(root, model_name +'_cln.sql')
                            file = open( path  , "w")
                            file.write("SELECT " +'\n'+  ", \n".join(str('\t'+ x) for x in schema_clean) + '\n' + 
                                        'FROM ' + model_name)
                            file.close()

if __name__ == "__main__":
    main()
