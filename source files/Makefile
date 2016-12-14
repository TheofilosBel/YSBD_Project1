OBJS1	= hash.o main_ex_HT.o
OBJS2	= exhash.o main_example_EH.o
OUT1	= execHT
OUT2	= execEH
CC		= gcc
FLAGS	= -c -Wall
LIB 	= ../BF/linux/BF_32.a

# test HT
t1: $(OBJS1)
	$(CC) $(OBJS1) $(LIB) -o $(OUT1)
hash.o: hash.c
	$(CC) $(FLAGS) hash.c
main_ex_HT.o: main_ex_HT.c
	$(CC) $(FLAGS) main_ex_HT.c

# test EH
t2: $(OBJS2)
	$(CC) $(OBJS2) $(LIB) -o $(OUT2)
exhash.o: exhash.c
	$(CC) $(FLAGS) exhash.c
main_example_EH.o: main_example_EH.c
	$(CC) $(FLAGS) main_example_EH.c


# clean up
clean:
	rm -f $(OBJS1) $(OUT1)
	rm -f $(OBJS2) $(OUT2)

