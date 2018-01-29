#include sources
set(libcts_SRCS
	infra/BitSet.cpp
	infra/Pool.cpp
	infra/QueryGraph.cpp
	parser/SPARQLLexer.cpp
	parser/SPARQLParser.cpp
	plangen/Plan.cpp
	plangen/PlanGen.cpp
	semana/SemanticAnalysis.cpp
)

#link sources
set(libcts_HDRS
	infra/BitSet.hpp
	infra/Pool.hpp
	infra/QueryGraph.hpp
	infra/VarPool.hpp
	parser/SPARQLLexer.hpp
	parser/SPARQLParser.hpp
	plangen/Costs.hpp
	plangen/Plan.hpp
	plangen/PlanGen.hpp
	semana/SemanticAnalysis.hpp
)



#link libraries
link_libraries(${Boost_LIBRARIES} rg mpi2 log4cxx)

file(COPY "${TriAD_SOURCE_DIR}/log4j.properties" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/")

#Build libraries
add_library(cts SHARED cts.hpp ${libcts_HDRS} ${libcts_SRCS})

# link with boost
target_link_libraries(cts ${Boost_LIBRARIES})

# install library and main headers
install(TARGETS cts DESTINATION ${LIB_INSTALL_DIR})
install(FILES ${libcts_HDRS} DESTINATION ${INCLUDE_INSTALL_DIR}/cts)
