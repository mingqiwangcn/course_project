db:
	g++ -g -o testcase testcase.c storage.c page_manager.c db_interface.c cluster_interface.c utils.c
import:
	g++ -g -o export_import export_import.c storage.c page_manager.c
py_db:
	g++ -shared -std=c++11 -fPIC `python3 -m pybind11 --includes` db_interface.c storage.c page_manager.c utils.c -o db_storage`python3-config --extension-suffix`
py_cluster:
	g++ -shared -std=c++11 -fPIC `python3 -m pybind11 --includes` cluster_interface.c storage.c page_manager.c utils.c -o db_cluster`python3-config --extension-suffix`

clean:
	rm  testcase 
