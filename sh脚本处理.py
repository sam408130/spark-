import re

data = open('list','r')
for i in data:
	i = i.strip('\n')
	mat = re.compile(r'/').split(i)
	l = len(mat)
	if re.search(r'\.lzo',i):
		newfile = re.sub(r'\.lzo','',mat[l-1])
		path = '/'.join(mat[1:l-1])
		print 'lzop -cd %s > %s'%(i,newfile)
		print 'scp %s sparkuser@serverx33:~/InputMethod/%s'%(newfile,path)
		print 'rm %s'%newfile


