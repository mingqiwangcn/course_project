all:
	g++ -g -o testcase testcase.c storage.c page_manager.c
py:
	g++ -shared -std=c++11 -fPIC `python3 -m pybind11 --includes` python_interface.c storage.c page_manager.c -o db_storage`python3-config --extension-suffix`
clean:
	rm  testcase 
