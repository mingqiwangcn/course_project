#include <string>
#include <unordered_map>
using namespace std;

string get_opt(unordered_map<string, string>& options, string opt_name) {
    unordered_map<string, string>::iterator itr;
    itr = options.find(opt_name);
    if (itr != options.end()) {
        return itr->second;
    } else {
        return "";
    }
}


