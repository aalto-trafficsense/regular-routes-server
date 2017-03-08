

# logging helpers
LOG_IMPORTANT = True # generate extensive logs!
LOG_DETAILS = False # generate extensive logs!
LOG_ERRORS = True # generate extensive logs!

def logmultiple(params): # base multiple-param logging function
    for param in params:
        print param,
    else:
        print

def log(params):
    if LOG_DETAILS: logmultiple(params)
    
def loge(params): # log errors and warnings
    if LOG_ERRORS: logmultiple(params)

def logi(params): # log important data or messages
    if LOG_IMPORTANT: logmultiple(params)

