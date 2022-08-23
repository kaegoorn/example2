macro(generate_flatbuffers_files)
        set(oneValueArgs TARGET OUTPUT PROTO_INCLUDE_DIR GENERATED_INCLUDE_DIR)
        set(multiValueArgs SOURCES CPP_SOURCES)
        cmake_parse_arguments(GENERATE_FLATBUFFERS_FILES "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN} )
        if(NOT GENERATE_FLATBUFFERS_FILES_SOURCES)
            message(FATAL_ERROR "SOURCES is not set")
        endif()
        if(NOT GENERATE_FLATBUFFERS_FILES_TARGET)
            message(FATAL_ERROR "TARGET is not set")
        endif()
        if(NOT GENERATE_FLATBUFFERS_FILES_OUTPUT)
            set(GENERATE_FLATBUFFERS_FILES_OUTPUT ${CMAKE_CURRENT_BINARY_DIR})
        endif()

        find_program(GENERATE_FLATBUFFERS_GENERATOR NAMES flatc PATHS ${CONAN_BIN_DIRS})
        if(NOT GENERATE_FLATBUFFERS_GENERATOR)
            message(FATAL_ERROR "Unable to find flatbuffers generator")
        endif()
        set(GENERATE_FLATBUFFERS_FILES_GENERATED_SOURCES "")
        set(GENERATE_FLATBUFFERS_FILES_GENERATED_HEADERS "")
        foreach(file ${GENERATE_FLATBUFFERS_FILES_SOURCES})
            get_filename_component(filename ${file} NAME_WE)
            set(filename "${GENERATE_FLATBUFFERS_FILES_OUTPUT}/${filename}")
            list(APPEND GENERATE_FLATBUFFERS_FILES_GENERATED_SOURCES "${filename}.grpc.fb.cc" "${filename}_generated.h")
            list(APPEND GENERATE_FLATBUFFERS_FILES_GENERATED_HEADERS "${filename}.grpc.fb.h")
        endforeach()

        list(APPEND GENERATE_FLATBUFFERS_FILES_GENERATED_SOURCES "${GENERATE_FLATBUFFERS_FILES_OUTPUT}/_dummy_.cc")
        set(GENERATE_FLATBUFFERS_FILES_GENERATED_FILES ${GENERATE_FLATBUFFERS_FILES_GENERATED_HEADERS} ${GENERATE_FLATBUFFERS_FILES_GENERATED_SOURCES})

        add_custom_command(OUTPUT ${GENERATE_FLATBUFFERS_FILES_GENERATED_FILES}
            BYPRODUCTS ${GENERATE_FLATBUFFERS_FILES_GENERATED_FILES}
            COMMENT "Generate flatbuffer files ${GENERATE_FLATBUFFERS_FILES_SOURCES}"
            COMMAND
                mkdir -p ${GENERATE_FLATBUFFERS_FILES_OUTPUT}
            COMMAND
                touch ${GENERATE_FLATBUFFERS_FILES_GENERATED_FILES}
            COMMAND
                ${GENERATE_FLATBUFFERS_GENERATOR} -o \"${GENERATE_FLATBUFFERS_FILES_OUTPUT}\" -I \"${GENERATE_FLATBUFFERS_FILES_PROTO_INCLUDE_DIR}\" --grpc --cpp --scoped-enums ${GENERATE_FLATBUFFERS_FILES_SOURCES} --keep-prefix
            DEPENDS
                ${GENERATE_FLATBUFFERS_FILES_SOURCES}
            WORKING_DIRECTORY
                ${CMAKE_CURRENT_LIST_DIR}
        )

        add_library(${GENERATE_FLATBUFFERS_FILES_TARGET} STATIC ${GENERATE_FLATBUFFERS_FILES_SOURCES} ${GENERATE_FLATBUFFERS_FILES_GENERATED_HEADERS})
        target_sources(${GENERATE_FLATBUFFERS_FILES_TARGET}
            PRIVATE ${GENERATE_FLATBUFFERS_FILES_GENERATED_SOURCES}
            PUBLIC ${GENERATE_FLATBUFFERS_FILES_CPP_SOURCES}
            )
        target_include_directories(${GENERATE_FLATBUFFERS_FILES_TARGET}
            PUBLIC
                ${CORE_BASE_DIR}/sources
                ${GENERATE_FLATBUFFERS_FILES_OUTPUT}/..
                ${GENERATE_FLATBUFFERS_FILES_GENERATED_INCLUDE_DIR}
                )
        target_link_libraries(${GENERATE_FLATBUFFERS_FILES_TARGET} PUBLIC
            ${CONAN_LIBS}
        )

endmacro()


