warmup2: warmup2.c
	gcc -o warmup2 warmup2.c -lpthread -lm -Wall

clean:
	rm -f warmup2
