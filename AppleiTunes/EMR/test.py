txt= ['PZB1_D_20240307_File 1 - Detailed Activity Metrics.txt',
'PZB1_D_19700101_20240307_File 2 - Detailed Content Identifiers.txt',
'PZB1_D_20240307_File 3 - Vendor,Product Details.txt',
'PZB1_D_19700101_20240307_File 4 - User Details.txt',
'PZB1_D_20240307_File 5 - User Activity Metrics.txt',
'PZB1_D_19700101_20240307_File 6 - Stream Source Details.txt',
'PZB1_D_19700101_20240307_File 7 - Detailed Station,Playlist,Chart Metrics.txt']

file=[]
for i in txt:
    file.append(i.split())
print(file)