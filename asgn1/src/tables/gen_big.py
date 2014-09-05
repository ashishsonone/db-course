from random import randint
import sys
b1 = open("b1.txt", 'w')
b2 = open("b2.txt", 'w')

b1.write("CREATE b1 ATTRS int char 10 char 20 double\n")
b2.write("CREATE b2 ATTRS int char 10 double\n")

count1 = int(sys.argv[1])
modulo1 = int(sys.argv[2])
count2 = int(sys.argv[3])
modulo2 = int(sys.argv[4])

for i in range(count1):
    x = randint(0, modulo1)
    b1.write("INSERT b1 VALUES " +  str(x) + " " +  str(1990 + x) +  " Korth" + str(x) + " " + str(randint(0,x)) + "\n")

for i in range(count2):
    x = randint(0, modulo2)
    b2.write("INSERT b2 VALUES " +  str(x) + " " +  str(1990 + x) +  " " + str(randint(0,x)) + "\n")

b1.write("quit\n")
b2.write("quit\n")
b1.close()
b2.close()
