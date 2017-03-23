import pandas

folderPath = "~/Projects/RKoning_CB/"
fileName = "organizations_sample_load_part2.csv"

def main():
    dat = pandas.read_csv(folderPath + fileName, index_col=0)
    print("Rows = " + str(dat.shape[0]) + ", Columns = " + str(dat.shape[1]))
    return

main()
exit()