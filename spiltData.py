import getopt
import json
import sys
import subprocess
import os

def main(argv):
        num_collections = 0
        file_to_data =""
        collection_prefix=""
        scope_name = 0
        start_value = 0
        opts =""
        args =""
        ind = 0
        data =[]
        path = "/fts/backup/exportFiles/temp.json"
        while ind < len(argv):
          if argv[ind] == "--num_col":
            ind = ind +1
            num_collections = int(argv[ind])
          if argv[ind] == "--collection_prefix" :
            ind = ind +1
            collection_prefix = argv[ind]
          if argv[ind] == "--scope_name":
            ind = ind+1
            scope_name = argv[ind]
          if argv[ind] == "--data_file":
            ind = ind + 1
            file_to_data = argv[ind]
          ind = ind + 1
        with open(file_to_data,"r")as fp:
          data = json.load(fp)
        num_docs_in_parts = int(len(data)/num_collections)
        print(num_docs_in_parts , int(num_docs_in_parts))
        end_val = num_docs_in_parts
        data_in_collection =[]
        for coll_num in range(0, num_collections):
          print("the start_value = {} , end_val = {} , len(data) = {} ".format(str(start_value), str(end_val), str(len(data))))
          if coll_num == num_collections-1:
            end_val = len(data)
          data_in_collection = [doc for doc in data[start_value:end_val]]
          with open(path,"w") as temp_fp:
            json.dump(data_in_collection, temp_fp)
          print("after dumping it to a file")
          bucket_name = "bucket-1"
          scope_collection_exp = scope_name +"."+collection_prefix+str(coll_num+1)
          print ("the scope_collection_exp is {}".format(scope_collection_exp))
          cmd = "/opt/couchbase/bin/cbimport json " \
                "--cluster http://localhost:8091 "\
                "--bucket {} " \
                "--dataset file://{} --threads 24 "\
                "--scope-collection-exp {} "\
                "--username Administrator --password password " \
                "--generate-key '#MONO_INCR#' --format list".format(
                  bucket_name,
                  path,
                  scope_collection_exp
                  )
          print("Running: {}".format(cmd))
          subprocess.run([cmd],shell=True)
          start_value = end_val
          end_val = end_val+num_docs_in_parts

if __name__ == '__main__':
    main(sys.argv)
