OBJS1	= hash.o main_example_HT.o
OBJS2	= exhash.o main_example_EH.o
OUT1	= execHT
OUT2	= execEH
CC		= gcc
FLAGS	= -c -Wall
LIB 	= BF_64.a

# test HT
hash.o: hash.c
	$(CC) $(FLAGS) hash.c
main_example_HT.o: main_example_HT.c
	$(CC) $(FLAGS) main_example_HT.c
t1: $(OBJS1)
	$(CC) $(OBJS1) $(LIB) -o $(OUT1)

# test EH
exhash.o: exhash.c
	$(CC) $(FLAGS) exhash.c
main_example_EH.o: main_example_EH.c
	$(CC) $(FLAGS) main_example_EH.c
t2: $(OBJS2)
	$(CC) $(OBJS2) $(LIB) -o $(OUT2)

# clean up
clean:
	rm -f $(OBJS1) $(OUT1)
	rm -f $(OBJS2) $(OUT2)