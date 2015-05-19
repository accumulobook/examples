#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import csv
import math
import uuid

import common
from accumulo.ttypes import *

client = None
login = None

def loadMLFile(filename):
    if client.tableExists(login, 'ml'):
        client.deleteTable(login, 'ml')
    client.createTable(login, 'ml', True, TimeType.MILLIS)
        
    opts = WriterOptions(1000000, 5000, 30000, 10)
    writer = client.createWriter(login, 'ml', opts)
    
    print 'loading file ...'
    with open(filename,'r') as f:
        reader = csv.reader(f,delimiter='\t')
        
        for userId, itemId, rating, timestamp in reader:
            
            # entity to feature
            m = {'user_' + userId: [ColumnUpdate('movie', 'item_' + itemId, value=rating)]}
            client.update(writer, m)
            
            # write feature to entity 
            im = {'item_' + itemId: [ColumnUpdate('movie', 'user_' + userId, value=rating)]}
            client.update(writer, im)
                
    client.closeWriter(writer)


def loadMoviesFile(filename):
    if client.tableExists(login, 'movies'):
        client.deleteTable(login, 'movies')
    client.createTable(login, 'movies', True, TimeType.MILLIS)

    opts = WriterOptions(1000000, 5000, 30000, 10)
    writer = client.createWriter(login, 'movies', opts)

    # movie id | movie title | release date | video release date | 
    # IMDb URL | unknown | Action | Adventure | Animation |
    # Children's | Comedy | Crime | Documentary | Drama | Fantasy |
    # Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
    # Thriller | War | Western |
    
    print 'loading file ...'
    with open(filename,'r') as f:
        reader = csv.reader(f,delimiter='|')

        for rec in reader:
            print rec
            # entity to feature
            m = {'item_' + rec[0] : [ColumnUpdate('movie', 'title', value='movie_' + rec[1])]}
            client.update(writer, m)

            # write feature to entity 
            im = {'movie_' + rec[1] : [ColumnUpdate('movie', 'id', value='item_' + rec[0])]}
            client.update(writer, im)

    client.closeWriter(writer)
    
def loadTsvFile(filename, fields=None):
    
    print 'loading file ...'
    with open(filename,'r') as f:
        if fields is None:
            fields = next(f)
        reader = csv.reader(f,delimiter='\t')
        
        for record in reader:
            
            # convert records into mutations
            rowId = str(uuid.uuid3(uuid.NAMESPACE_OID, ''.join(record)))
            m = {rowId: [ColumnUpdate('', x[0], value=x[1]) for x in zip(fields, record)]}
            client.update(writer, m)
            
            # write index entries
            for i in range(len(record)):
                value = record[i]
                field = fields[i]
                im = {value: [ColumnUpdate(field, rowId, value='')]}
                client.update(indexWriter, im)
                
        client.flush(writer)

    
def cosineSim(a, b):
    
    maga = math.sqrt(sum([x * x for x in map(float, a.values())]))
    magb = math.sqrt(sum([x * x for x in map(float, b.values())]))
    d = 0.0
    
    for k,v in a.items():
        d += float(b.get(k, 0.0)) * float(v)
        
    return d / (maga * magb)

def lookupMovie(mid):
    return common.scan('movies', mid, mid+'\0')[0].value
    
def lookupMovieId(movie):
    return common.scan('movies', movie, movie+'\0')[0].value
        
def getFeatures(table, entity):
    return dict([(e.key.colQualifier,e.value) for e in common.scan(table, entity, entity + '\0')])

def kNearestNeighbors(table, rowId, k):
    
    topK = [(0.0,None)] * k
    
    # get features of rowId
    features = getFeatures(table, rowId)
    print 'got features', '\n'.join(['\t'.join((lookupMovie(x[0]), x[1])) for x in features.items()])
    
    # get other entities with at least one feature in common
    others = set([e.key.colQualifier for e in common.batchScan(table, features.keys())])
    others.remove(rowId)
    
    print 'got ', len(others), 'others'
    
    # grab each entity's feature vector
    processed = 0
    for other in others:
        otherFeatures = getFeatures(table, other)
        
        d = cosineSim(features, otherFeatures)
        #print d, other
        
        topK.append((d,other,otherFeatures))
        processed += 1
        if processed % 100 == 0:
            print 'sorting topk'
            topK.sort(reverse=True)
            topK = topK[:k]
    
    topK.sort(reverse=True)
    topK = topK[:k]
    return topK

def recommendedScore(nearest, movie):
    item = lookupMovieId(movie)
    score = 0.0
    matched = 0
    for n in nearest:
        items = n[2]
        if items.has_key(item):
            matched += 1
            score += float(items[item])
    # average
    if matched > 0:
        score = score / matched
    return score


if __name__ == '__main__':
    
    common.connect()
    
    client = common.client
    login = common.login
    
    #loadMLFile(sys.argv[1])
    #loadMoviesFile(sys.argv[1])
    
    # find 10 nearest neighbors (similar viewers)
    #nearest = kNearestNeighbors('ml', 'user_1', 10)

    # predict rating for a given movie
    #predictedScore = recommendedScore(nearest, '')

    
