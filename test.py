import time
import sys

i = 0
while i < 100:
    time.sleep(5)
    i = i+1
    print("Iteracao "+str(i))
    sys.stdout.flush()
