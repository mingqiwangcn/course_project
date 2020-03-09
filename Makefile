db:
	g++ -g -o testcase testcase.c storage.c page_manager.c db_interface.c
py_db:
	g++ -shared -std=c++11 -fPIC `python3 -m pybind11 --includes` db_interface.c storage.c page_manager.c -o db_storage`python3-config --extension-suffix`
py_cluster:
	g++ -shared -std=c++11 -fPIC `python3 -m pybind11 --includes` cluster_interface.c storage.c page_manager.c -o db_cluster`python3-config --extension-suffix`

clean:
	rm  testcase 
