import json
import _io
import time


filename = "sw1.json"
folderPath_root_cb = "C:\\Users\\pjonak\\Documents\\Projects\\" + \
                     "Koning\\Crunchbase_SimilarWeb_BuiltWith\\"

hFile = _io.open(folderPath_root_cb + filename)
dat = json.loads(hFile.read())
hFile.close()

# print(dat)

print(dat)
print(dat['visits'])
print(dat['visits'][0])
print(len(dat['visits']))
print(dat['visits'][1])
print(dat['visits'][2])
print(dat['visits'][3])
print(len(dat))
print(dat['meta'])


dat2 =





# hFile = _io.open(folderPath_root_cb + "write.json", "w")
# json.dump(dat,hFile)
# hFile.close()
#
# time.sleep(0.1)
#
# hFile = _io.open(folderPath_root_cb + "write.json")
# dat = json.loads(hFile.read())
# hFile.close()
#
# print(dat)