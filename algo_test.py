


#precalculate values at night time for maximum?

#using dynamo or athena data for each segmentId

#append the result into a database 

#calculate score
def calculateScore(locationHash, audienceId):
    score = 0
#access the precalculated values in the above database to compare the count of locationHash with the precalculated statistic for that audienceId
        

#score and save the array of all the result for each audienceId and calulate the average
        

#return the result 
    return score


def getIntegratedAdomniScore(segId_by_normalizedScore):
    integratedAdomniScore = 0
    for segId in segId_by_normalizedScore:
        integratedAdomniScore = integratedAdomniScore + segId_by_normalizedScore[segId]
    return integratedAdomniScore/len(segId_by_normalizedScore)