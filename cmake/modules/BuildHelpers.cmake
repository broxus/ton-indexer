# ############################################################### #
# Args:
#   Option:
#       IF_DIFFERENT        copy files if different (optional)
#   One arg:
#       TARGET              files will be copied before build this target
#       DESTINATION         folder destination
#   Multi args:
#       FILES_PATTERNS      files for copy, regex suppoted
# ############################################################### #
function(copy_files_before_build)
    set(options IF_DIFFERENT)
    set(oneValueArgs TARGET DESTINATION)
    set(multiValueArgs FILES_PATTERNS)
    cmake_parse_arguments(BH "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")

    file(GLOB FILES ${BH_FILES_PATTERNS})

    set(_COMMAND copy)
    if (${BH_IF_DIFFERENT})
        set(_COMMAND copy_if_different)
    endif()

    set(COPY_TARGETS "")

    foreach(FILE ${BH_FILES_PATTERNS})
        get_filename_component(CURRENT_TARGET ${FILE} NAME_WE)
        add_custom_target(${CURRENT_TARGET}
            COMMAND ${CMAKE_COMMAND} -E ${_COMMAND} "${FILE}" "${BH_DESTINATION}")

        set(COPY_TARGETS ${COPY_TARGETS} ${CURRENT_TARGET})
    endforeach()

    add_dependencies(${BH_TARGET} ${COPY_TARGETS})
endfunction()