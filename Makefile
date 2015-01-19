CC=gcc
CFLAGS= --std=c99 -D_XOPEN_SOURCE -D_GNU_SOURCE
INCLUDE=-I. -I /usr/local/include/libbson-1.0 -I /usr/local/include/libmongoc-1.0

DEPS = 
OBJ = main.o 
LIBS = -lmongoc-1.0 -lbson-1.0

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(INCLUDE) $(CFLAGS)

datagen : $(OBJ)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)

clean:
	rm -f *.o datagen
