import os

with open('application.yml', 'w') as yml:
    path = "/Users/hayde/IdeaProjects/drools/data/src/main/resources/data/test/"
    for get_correlate_dir in path:
        for file in os.listdir(path+get_correlate_dir):
            print(file)
            if os.path.isdir(path+get_correlate_dir+"/"+file):
                print("hello!")
                yml.write(get_correlate_dir + ":")
                yml.write("\n")
                yml.write("\t")
                for csvs in os.listdir(path+get_correlate_dir+"/"+file):
                    if ".csv" in csvs:
                        no_csv = csvs.split(".")[0]
                        yml.write(" - " + no_csv)
                        yml.write("\n")
                        yml.write("\t")
        yml.write("\n")
    yml.flush()