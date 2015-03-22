import numpy as np

def majorityVoting(lst):
    N = len(lst[0]); M = len(lst)
    pred = np.zeros(N)
    
    for i in range(N):
        for j in range(M):
            pred[i] += lst[j][i]       
    pred = pred/M
    ##decision boundary
    for i in range(N):
        if pred[i] < 0.5: 
            pred[i] = 0
        else: 
            pred[i] = 1  
    return pred