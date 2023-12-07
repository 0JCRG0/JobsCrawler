from indeed import indeed
import timeit
#import sys
#sys.path.append('/Users/juanreyesgarcia/Library/CloudStorage/OneDrive-FundacionUniversidaddelasAmericasPuebla/DEVELOPER/PROJECTS/CRAWLER_ALL/utils')

#from handy import *

def MASTER():
    KEYWORDS_LIST = [
        "DATA+ANALYST",
        "DATA",
        "MACHINE+LEARNING",
        "PYTHON",
        "BUSINESS+ANALYST",
        "ANALISTA+DE+DATOS",
        "INGENIERO+DE+DATOS",
        "DATA+ENGINEER",
        "SQL",
        "BUSINESS+INTELLIGENCE",
        "DATABASE+ADMINISTRATOR",
        "ABOGADO",
        "Big+Data"
        ]
    SCHEME = "specific_mx"
    
    # start master timer
    start_time = timeit.default_timer()

    #loop to get the jobs 
    for KEYWORD in KEYWORDS_LIST:
        indeed(SCHEME, KEYWORD)
        print("\n", "MOVING ON...","\n")
    

    #print the time
    end_time = timeit.default_timer()
    elapsed_time_minutes = (end_time - start_time) / 60
    print("\n", f"DONE! mx.indeed's crawler finished in: {elapsed_time_minutes:.1f} minutes, not bad", "\n")

if __name__ == "__main__":
    MASTER()