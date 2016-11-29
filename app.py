import csv, pprint, json, datetime, pandas

def convertUnixTime(unixtime, get='datetime'):
	if get == 'date':
		return datetime.datetime.fromtimestamp(int(unixtime)).strftime('%d-%m-%Y')
	elif get == 'time':
		return datetime.datetime.fromtimestamp(int(unixtime)).strftime('%H:%M:%S')
	else:
		return datetime.datetime.fromtimestamp(int(unixtime)).strftime('%d-%m-%Y %H:%M:%S')


def processingFile(file, limit = 0):
	print "Do Processing File..."
	data = {}
	with open(file, 'rb') as csvfile:
		reader = csv.DictReader(csvfile)
		i = 0
		for row in reader:
			i += 1
			print "Data ke",i
			if row['identity'] not in data.keys():
				user = {
					row['identity']: {
						row['session_id']: {
							'category': [row['param_category_slugs']],
							'unixtime': [row['time']],
							'datetime': [convertUnixTime(row['time'])],
							'via': [row['subdomain']]
						}
					}
				}
				data.update(user)
			else:
				if row['session_id'] not in data[row['identity']].keys(): 
					user = data[row['identity']]
					session = {
						row['session_id']: {
							'category': [row['param_category_slugs']],
							'unixtime': [row['time']],
							'datetime': [convertUnixTime(row['time'])],
							'via': [row['subdomain']]
						}	
					}
					user.update(session)
				else:
					session = data[row['identity']][row['session_id']]
					session['category'].append(row['param_category_slugs'])
					session['unixtime'].append(row['time'])
					session['datetime'].append(convertUnixTime(row['time']))
					session['via'].append(row['subdomain'])
			if limit != 0:
				if i >= limit:
					break
		return data

def getAllCategory(file):
	print "Get All Category Processing..."
	data = {}
	f = pandas.read_csv(file)
	cat = f.param_category_slugs
	for i in cat:
		if i not in data.keys():
			data[i] = 1
		else:
			data[i] += 1
	return data

def writeToJson(file, data):
	print "Write to JSON Processing..."
	j = json.dumps(data, indent=2)
	f = open(file, 'w')
	print >> f,j
	f.close()

# cat = getAllCategory('training-100k.csv')
# writeToJson('category.json', cat)

a = processingFile('training-100k.csv')
writeToJson('train.json',a)