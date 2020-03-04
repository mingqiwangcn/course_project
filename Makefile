all:
	g++ -g -o testcase testcase.c storage.c page_manager.c

clean:
	rm  testcase 
