# ############################################################### #
# Headeronly options ############################################ #
# ############################################################### #
macro(generate_basic_options_headeronly NAME)
    set(
        ${NAME}_INSTALL_CMAKE_PREFIX
        "lib/cmake/${NAME}"
        CACHE PATH "Installation directory for CMake files")
    set(
        ${NAME}_INSTALL_INCLUDE_PREFIX
        "include/${NAME}"
        CACHE PATH "Installation directory for header files")
endmacro()

# ############################################################### #
# Library options ############################################### #
# ############################################################### #
macro(generate_basic_options_library NAME)
    option(${NAME}_BUILD_SHARED "Build the ${NAME} as shared." OFF)
    set(
        ${NAME}_INSTALL_CMAKE_PREFIX
        "lib/cmake/${NAME}"
        CACHE PATH "Installation directory for CMake files")
    set(
        ${NAME}_INSTALL_INCLUDE_PREFIX
        "include/${NAME}"
        CACHE PATH "Installation directory for header files")
    set(
        ${NAME}_INSTALL_BIN_PREFIX
        "bin"
        CACHE PATH "Installation directory for executables")
    set(
        ${NAME}_INSTALL_LIB_PREFIX
        "lib"
        CACHE PATH "Installation directory for libraries")
endmacro()

# ############################################################### #
# Executable options ############################################ #
# ############################################################### #
macro(generate_basic_options_executable NAME)
    set(
        ${NAME}_INSTALL_BIN_PREFIX
        "bin"
        CACHE PATH "Installation directory for executables")
    set(
        ${NAME}_INSTALL_LIB_PREFIX
        "lib"
        CACHE PATH "Installation directory for libraries")
endmacro()