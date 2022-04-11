import json



def get_profile():
    f=open("profiles.json","r")
    profiles=json.load(f)
    f.close()
    return profiles 

def get_cost():
    f=open("ping_statistics.json","r")
    cost=json.load(f)
    f.close()
    return cost

