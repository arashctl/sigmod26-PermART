#include <iostream> 


#define COUTATOMIC(coutstr) /*cout<<coutstr*/ \
{ \
    std::stringstream ss; \
    ss<<coutstr; \
    std::cout<<ss.str(); \
}

int main() {

    return 0;
}