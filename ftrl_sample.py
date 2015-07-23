# ad click prediction : a view from the trenches
# __author__ : Abhishek Thakur
# __credits__ : tinrtgu


from math import sqrt, exp, log
from csv import DictReader

import sqlite3
import datetime

class ftrl(object):

	def __init__(self, alpha, beta, l1, l2, bits):
		
            self.z = [0.] * bits
	    self.n = [0.] * bits
	    self.alpha = alpha
	    self.beta = beta
	    self.l1 = l1
	    self.l2 = l2
	    self.w = {}
            self.X = []
	    self.y = 0.
	    self.bits = bits
	    self.Prediction = 0.
	
	def sgn(self, x):
		if x < 0:
			return -1  
		else:
			return 1

	def fit(self, line):
		
                try:
			self.ID = line['SearchID']
			del line['SearchID']
		except TypeError:
			pass

		try:
			self.y = float(line['IsClick'])
			del line['IsClick']
		except:
			pass

		del line['HistCTR']
		self.X = [0.] * len(line)
		for i, key in enumerate(line):
                        #print 'Debug XXXX', key, line[key]
                        try:
                            val = str(line[key])
                        except UnicodeEncodeError:
                            val = str(line[key].encode('utf8'))
			self.X[i] = (abs(hash(key + '_' + val)) % self.bits)
		self.X = [0] + self.X

	def logloss(self):
		act = self.y
		pred = self.Prediction
		predicted = max(min(pred, 1. - 10e-15), 10e-15)
		return -log(predicted) if act == 1. else -log(1. - predicted)

	def predict(self):
		
                W_dot_x = 0.
		w = {}
		for i in self.X:
			if abs(self.z[i]) <= self.l1:
				w[i] = 0.
			else:
				w[i] = (self.sgn(self.z[i]) * self.l1 - self.z[i]) / (((self.beta + sqrt(self.n[i]))/self.alpha) + self.l2)
			W_dot_x += w[i]
		
                self.w = w
		self.Prediction = 1. / (1. + exp(-max(min(W_dot_x, 35.), -35.)))
		
                return self.Prediction

	def update(self, prediction): 
		for i in self.X:
			g = (prediction - self.y) #* i
			sigma = (1./self.alpha) * (sqrt(self.n[i] + g*g) - sqrt(self.n[i]))
			self.z[i] += g - sigma*self.w[i]
			self.n[i] += g*g

        def joinTable(self, tableA):
            
            query_command = '''select * from 
            ''' + tableA + ''' X
            inner join AdsInfo Y
            on X.AdID = Y.AdID
            inner join VisitsStream Z1
            on X.AdID = Z1.AdID
            '''

            print query_command

            return query_command
            

if __name__ == '__main__':

	"""
	SearchID	AdID	Position	ObjectType	HistCTR	IsClick
	"""
        
        start = datetime.datetime.now()
	
        
        database = '../data/database.sqlite'
	clf = ftrl(alpha = 0.1, 
                beta = 1., 
		l1 = 0.4,
                l2 = 1.0, 
		bits = 24)

	loss = 0.
	count = 0

        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        
        print 'Joining table:'
        train = 'trainSearchStream'
        #query = 'select * from ' + train #+ ' limit 10'
        query = clf.joinTable(train)
        print 'Table join takes ', datetime.datetime.now() - start
        cursor.execute(query)

        #tabs = ['AdsInfo']
        names = [description[0] for description in cursor.description ]
        print names
        line = {}
        for name in names:
            line[name] = 0

        print 'Training FTRL model:'
	while 1:
            
            values = cursor.fetchone()
            
            if values is None:
                break
            for ind in xrange(len(names)):
                line[names[ind]] = values[ind]

            #print 'Debug:', line, type(line)

            clf.fit(line)
	    pred = clf.predict()
	    loss += clf.logloss()
	    clf.update(pred)
	    count += 1
	    if count%100000 == 0: 
		    print ("(seen, loss) : ", (count, loss * 1./count))
	    #if count == 100000: 
		    #break

        print 'Model training takes ', datetime.datetime.now() - start

        print 'Export testing results:'
        test = 'testSearchStream'
        #query = 'select * from ' + test #+ ' limit 10'
        query = clf.joinTable(test)
        cursor.execute(query)

        names = [description[0] for description in cursor.description ]
        print names
        line = {}
        for name in names:
            line[name] = 0


	with open('temp/temp_2.csv', 'w') as output:
            
            while 1:
            
                values = cursor.fetchone()
            
                if values is None:
                    break
                for ind in xrange(len(names)):
                    line[names[ind]] = values[ind]

                #print 'Test Debug:', line, type(line)

		clf.fit(line)
		output.write('%s\n' % str(clf.predict()))
        
        print 'Result export takes ', datetime.datetime.now() - start
